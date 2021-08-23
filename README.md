# SparkETL
基于Spark的ETL批量抽取入数程序

配置文件为config.properties

还有程序的参数，参考cn.hanpeng.StringUtil.check_args()

<p> -h 帮助</p>
<p> -l 是否是本地模式，默认为true </p>
<p> -n Spark任务名称 </p>
<p> -p 并行数 </p>
<p> -e spark.executor.memory,default is 1g </p>
<p> -k 任务的开始，可以是数字，也可以是日期，需要在-f参数中设置格式化的格式 </p>
<p> -j 任务的结束，可以是数字，也可以是日期，需要在-f参数中设置格式化的格式 </p>
<p> -f 日期格式化的格式，如果值为num，则作为数字进行格式化 </p>
<p> -r 是否读取日志，已经过时，默认为false，不推荐使用，如果为true，会读取日志文件中已经完成的任务，重新开始时将去除这些已经完成的任务，即是在任务发生异常终止时，重新启动时才需要设置为true，<b>此功能尚不完整</b>，不推荐设为true </p>
<p> -g 重新分区的分区数，用于spark的 repartition </p>
<p> -bs batchSize,即每次入数时的批次大小，默认为1000，即每1000条入一次库，默认会读取配置文件中的batchSize的值，如果存在此参数，则会按照该参数的值作为批次大小，命令优于配置文件 </p>
<p> -fs fetchSize,Jdbc的fatchSize 大小，默认为1000，命令优于配置文件 </p>
<p> -it intervalTime,批次的间隔时间，单位为秒，即假设批次时间为86400，则从20190101-20190103中间会产生2个批次 </p>
<p> -c 查询的SQL中有多少个列 </p>
<p> -ss selectSql,查询源库的SQL命令，命令优于配置文件，即如果存在此参数，则会覆盖从配置文件中读取selectSql的值 </p>
<p> -is insertSql,插入目标库的SQL命令，命令优于配置文件，即如果存在此参数，则会覆盖从配置文件中读取insertSql的值，<b>从源库查询的列的顺序与入目标库的列的顺序必须一致</b> </p>
<p>-config 配置文件路径，默认会读取jar包中的config.properties，如果存在此参数，则会读取该路径的配置文件 </p>
<p>-partitions 分区名称，多个用逗号分开，目前分区和开始时间结束时间不能同时使用 </p>
<p>-partitions 分区名称，多个用逗号分开，支持分区和开始时间结束时间同时使用，例如 -partitions A_201901:20190101-20190201,A_201902:20190201-20190301  假设时间间隔为86400秒，则会生成31+28个任务，规则为首先根据逗号分隔，分隔后，再根据冒号分隔获取时间区间，再根据横岗分隔获取开始时间结束时间，并生成对应的任务</p>