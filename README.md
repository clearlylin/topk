# 设计思路

## 分而治之

* 拆分文件，大文件拆分小文件
* 拆分计算，处理发送至不同计算节点
* 最后合并，取topk
* 与MapReduce计算架构类似, 内存限制，控制每步中内存使用量

## 文件拆分

* Hash函数选择问题，URL存在前缀相同问题，可否优化后部分求HASH
* 文件桶的形式存放，并行处理，文件写buffer，内存不足，写本地文件，
* 存在文件大于内存的情况，进行二次Hash拆分

## 统计计数

* 并行处理每个小文件，存在内存不足的情况，仍然可设置buffer，写本地文件
* 拆分的文件，肯定小于最大内存
* 单文件统计肯定OK，能否并行统计

## 排序topk

* 小根堆排序算法
* 单文件排序内存使用OK, 能否并行处理

## 归并排序两个合并

* 小根堆算法
* 归并排序

## 并行方式

* 流水线处理，split, statistic, sort, merge
* 并行合并流水线处理，涉及中间文件合并操作，稍微麻烦一些

## Test Data

[data Source address](http://www.cs.columbia.edu/CAVE/databases/pubfig/download/)