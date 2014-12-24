package com.linkedin.thirdeye.bootstrap.startree.generation;

import static com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationConstants.STAR_TREE_GEN_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.bootstrap.DimensionKey;
import com.linkedin.thirdeye.bootstrap.MetricSchema;
import com.linkedin.thirdeye.bootstrap.MetricType;
import com.linkedin.thirdeye.bootstrap.util.TarGzCompressionUtils;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;

/**
 * 
 * @author kgopalak
 * 
 */
public class StarTreeGenerationJob extends Configured {
  private static final Logger LOG = LoggerFactory
      .getLogger(StarTreeGenerationJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  public StarTreeGenerationJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class StarTreeGenerationMapper extends
      Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private StarTreeGenerationConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    MultipleOutputs<BytesWritable, BytesWritable> mos;
    Map<String, Integer> dimensionNameToIndexMapping;
    StarTreeManager starTreeManager;
    String collectionName;
    private String hdfsOutputPath;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOG.info("StarTreeGenerationJob.StarTreeGenerationMapper.setup()");
      mos = new MultipleOutputs<BytesWritable, BytesWritable>(context);
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(STAR_TREE_GEN_CONFIG_PATH
          .toString()));

      try {
        config = OBJECT_MAPPER.readValue(fileSystem.open(configPath),
            StarTreeGenerationConfig.class);
        dimensionNames = config.getDimensionNames();
        dimensionNameToIndexMapping = new HashMap<String, Integer>();

        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionNameToIndexMapping.put(dimensionNames.get(i), i);
        }
        metricNames = config.getMetricNames();
        metricTypes = Lists.newArrayList();
        for (String type : config.getMetricTypes()) {
          metricTypes.add(MetricType.valueOf(type));
        }
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);

        // set up star tree builder
        collectionName = config.getCollectionName();
        String timeColumnName = config.getTimeColumnName();
        List<String> splitOrder = config.getSplitOrder();
        int maxRecordStoreEntries = config.getSplitThreshold();
        StarTreeConfig config = new StarTreeConfig.Builder()
            .setCollection(collectionName) //
            .setDimensionNames(dimensionNames)//
            .setMetricNames(metricNames)//
            .setTimeColumnName(timeColumnName) //
            // .setSplitOrder(splitOrder)//
            .setMaxRecordStoreEntries(maxRecordStoreEntries).build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        starTreeManager = new StarTreeManagerImpl(executorService);
        starTreeManager.registerConfig(collectionName, config);
        starTreeManager.create(collectionName);
        starTreeManager.open(collectionName);
        hdfsOutputPath = context.getConfiguration().get(
            STAR_TREE_GEN_OUTPUT_PATH.toString())
            + "/" + "star-tree-" + collectionName;
        LOG.info(config.toJson());
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable dimensionKeyWritable,
        BytesWritable timeSeriesWritable, Context context) throws IOException,
        InterruptedException {
      // construct dimension key from raw bytes
      DimensionKey dimensionKey = DimensionKey.fromBytes(dimensionKeyWritable
          .copyBytes());
      Map<String, String> dimensionValuesMap = new HashMap<String, String>();
      for (int i = 0; i < dimensionNames.size(); i++) {
        dimensionValuesMap.put(dimensionNames.get(i),
            dimensionKey.getDimensionsValues()[i]);
      }
      Map<String, Integer> metricValuesMap = new HashMap<String, Integer>();
      for (int i = 0; i < metricNames.size(); i++) {
        metricValuesMap.put(metricNames.get(i), 0);
      }
      Long time = 0l;
      StarTreeRecord record = new StarTreeRecordImpl(dimensionValuesMap,
          metricValuesMap, time);
      starTreeManager.getStarTree(collectionName).add(record);

    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {
      // add catch all other node under every leaf.

      LOG.info("START: serializing star tree and the leaf record dimension store");
      StarTree starTree = starTreeManager.getStarTree(collectionName);
      String localOutputDir = "./star-tree-" + collectionName;
      // add catch all node to every leaf node
      Map<String, Integer> metricValuesMap = new HashMap<String, Integer>();
      for (int i = 0; i < metricNames.size(); i++) {
        metricValuesMap.put(metricNames.get(i), 0);
      }
      Long time = 0l;

      // get the leaf nodes
      LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
      starTree.close();
      StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTree.getRoot());
      int prevLeafNodes;
      do {
        prevLeafNodes = leafNodes.size();
        LOG.info("Number of leaf Nodes"+ prevLeafNodes);
        for (StarTreeNode node : leafNodes) {
          Map<String, String> ancestorDimensionValues = node
              .getAncestorDimensionValues();
          Map<String, String> map = new HashMap<String, String>();
          map.putAll(ancestorDimensionValues);
          map.put(node.getDimensionName(), node.getDimensionValue());
          // for the dimensions that are not yet split, set them to OTHER
          for (String dimensionName : dimensionNames) {
            if (!map.containsKey(dimensionName)) {
              map.put(dimensionName, StarTreeConstants.OTHER);
            }
          }
          // create the catch all record under this leaf node
          // TODO: the node might split after adding this record. we should
          // support a mode in star tree to stop splitting.
          StarTreeRecord record = new StarTreeRecordImpl(map, metricValuesMap,
              time);
          starTreeManager.getStarTree(collectionName).add(record);
        }
        // Adding a catch all node might split an existing leaf node and create
        // more leaf nodes, in which case we will have to add the catch all node
        // to the leaf nodes again. This will go on until no leaf nodes split
        // TODO: this is a temporary work around. Eventually we need to freeze
        // the tree and avoid further splitting
        leafNodes.clear();
        StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTree.getRoot());
        LOG.info("Number of leaf Nodes"+ prevLeafNodes);
      } while (prevLeafNodes != leafNodes.size());
      //close will invoke compaction
      starTree.close();

      FileSystem dfs = FileSystem.get(context.getConfiguration());
      Path src, dst;
      // generate tree and copy the tree to HDFS
      StarTreePersistanceUtil.saveTree(starTree, localOutputDir);
      String treeOutputFileName = collectionName + "-tree.bin";
      src = FileSystem.getLocal(new Configuration()).makeQualified(
          new Path(localOutputDir + "/" + treeOutputFileName));
      dst = dfs.makeQualified(new Path(hdfsOutputPath, treeOutputFileName));
      LOG.info("Copying " + src + " to " + dst);
      dfs.copyFromLocalFile(src, dst);

      // generate and copy leaf record to HDFS
      String leafDataOutputDir = localOutputDir + "/" + "data";
      new File(leafDataOutputDir).mkdirs();
      StarTreePersistanceUtil
          .saveLeafDimensionData(starTree, leafDataOutputDir);
      LOG.info("END: serializing the leaf record dimension store");
      String leafDataTarGz = localOutputDir + "/leaf-data.tar.gz";
      LOG.info("Generating " + leafDataTarGz + " from " + leafDataOutputDir);
      // generate the tar file
      TarGzCompressionUtils.createTarGzOfDirectory(leafDataOutputDir,
          leafDataTarGz);
      src = FileSystem.getLocal(new Configuration()).makeQualified(
          new Path(leafDataTarGz));
      dst = dfs.makeQualified(new Path(hdfsOutputPath, "leaf-data.tar.gz"));
      LOG.info("Copying " + src + " to " + dst);
      dfs.copyFromLocalFile(src, dst);

    }
  }

  public void run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeGenerationJob.class);

    // Map config
    job.setMapperClass(StarTreeGenerationMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);

    // Reduce config
    job.setNumReduceTasks(0);
    // rollup phase 2 config
    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration,
        StarTreeGenerationConstants.STAR_TREE_GEN_INPUT_PATH);
    getAndSetConfiguration(configuration,
        StarTreeGenerationConstants.STAR_TREE_GEN_CONFIG_PATH);
    getAndSetConfiguration(configuration,
        StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH);
    LOG.info("Running star tree generation job");
    LOG.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOG.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    FileOutputFormat.setOutputPath(
        job,
        new Path(
            getAndCheck(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH
                .toString())));

    job.waitForCompletion(true);
    LOG.info("Finished running star tree generation job");

  }

  private String getAndSetConfiguration(Configuration configuration,
      StarTreeGenerationConstants constant) {
    String value = getAndCheck(constant.toString());
    configuration.set(constant.toString(), value);
    return value;
  }

  private String getAndCheck(String propName) {
    String propValue = props.getProperty(propName);
    if (propValue == null) {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
  }
}