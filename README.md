# SparkStreaming
实时统计微博话题词频

运行步骤：

安装maven依赖

运行SparkStreamingKafka(需要保证HDFStoKafka项目在运行中，否则无法获取到流数据)

注意提交到yarn的话需要修改CHECK_POINT_DIR和setMaster的值