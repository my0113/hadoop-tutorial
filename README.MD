# 项目说明

## 1. 代码说明

### 1.1 代码结构
    cn.itcast.hadoop.mapreduce： 所有MapReduce实现代码
    cn.itcast.hadoop.yarn：      所有YARN模块实现代码
### 1.2 MapReduce
    ==== 所有MR作业均采用Configured和Tool方式实现（13个MR） ====
    MR的WordCount案例，未同步到讲义
        需求（没有，待补充）
        分析（有）
        数据（有，mapreduce-paper-part0.txt）
        实现（有，含1~7步骤，待补充）
            MR1代码：cn.itcast.hadoop.mapreduce.WordCountApp
    MR的SequenceFile操作，已同步到讲义《第四章-MapReduce框架和YARN（新）.docx》
        需求（有）
        分析（有）
        数据（有，mapreduce-paper-part0.txt，mapreduce-paper-part1.txt）
        实现（有，含1~7步骤）
            MR1代码：cn.itcast.hadoop.mapreduce.compress.sequencefile.ReadSeqFileApp
            MR2代码：cn.itcast.hadoop.mapreduce.compress.sequencefile.WriteSeqFileApp
    MR的GZip操作，未同步到讲义
        需求（有）
        数据（有，orders.csv）
        分析（有）
        实现（有，不含步骤，待补充）
            MR1代码：cn.itcast.hadoop.mapreduce.compress.gzip.ReadGZipApp
            MR2代码：cn.itcast.hadoop.mapreduce.compress.gzip.WriteGZipApp
    MR的DB操作，未同步到讲义
        需求（有）
        数据（有，orders.csv）
        分析（有）
        实现（有，不含步骤，待补充）
            MR1代码：cn.itcast.hadoop.mapreduce.db.ReadDBApp
            MR2代码：cn.itcast.hadoop.mapreduce.db.WriteDBApp
        案例（有，一个模仿Sqoop导入导出的例子）
            主程序：cn.itcast.hadoop.mapreduce.db.imitate.SqoopApp
            导出Map：cn.itcast.hadoop.mapreduce.db.imitate.mapper.ExportMapper
            导入Map：cn.itcast.hadoop.mapreduce.db.imitate.mapper.ImportMapper
    MR的工作流操作，未同步到讲义
        需求（有）
        数据（有）
        分析（有，orders.csv）
        实现（有，不含步骤，待补充）
            MR1代码：cn.itcast.hadoop.mapreduce.jobflow.MRApp1
            MR2代码：cn.itcast.hadoop.mapreduce.jobflow.MRApp2
            作业流代码：cn.itcast.hadoop.mapreduce.jobflow.JobFlowApp
    MR的JoinSide操作，未同步到讲义
        需求（有）
        数据（有）
        分析（有，orders.csv、pay.csv）
        实现（有，不含步骤，待补充）
            MR1代码：cn.itcast.hadoop.mapreduce.join.MapperJoinApp
            MR2代码：cn.itcast.hadoop.mapreduce.join.ReducerJoinApp
    MR的UBer模式操作，未同步到讲义
        需求（有）
        数据（有）
        分析（有）
        数据（有，mapreduce-paper-part0.txt，mapreduce-paper-part1.txt）
            MR1代码：cn.itcast.hadoop.mapreduce.uber.UBerApp
    MR的LZO与Snappy压缩，部分实现，未同步到讲义
        需求（无）
        数据（有）
        分析（无）
            LZO未完成，代码仅写了一部分，需要配合操作系统的本地库（Lzo）结合以及自行编译hadoop-lzo源码来配合使用
            Snappy未完成，代码未开始，需要配合操作系统的本地库（Snappy）结合以及自行编译Snappy客户端源码来配合使用
    MR的MapFile与OrcFile压缩，未完成，未同步到讲义
        需求（无）
        数据（无）
        分析（无）

### 1.3 YARN
    
## 2. 数据说明
    所有的数据文件位于项目根目录的data文件夹下
### 2.1 orders.csv
    订单信息表
    包含字段：
        id                      // ID
        orderSn                 // 订单编号
        state                   // 订单状态
        goodsId                 // 商品ID
        goodsName               // 商品名称
        goodsAttr               // 商品属性
        goodsCount              // 商品数量
        goodsTotalAmount        // 商品总价
        shopDiscount            // 店铺优惠
        platfromDiscount        // 平台优惠
        expressAmount           // 快递费
        expressType             // 快递类型
        expressName             // 快递名称
        SMAZF                   // 上门安装费
        SHRHF                   // 送货入户费
        SHRHAZF                 // 送货入户并安装费
        actualAmount            // 实收金额
        idName                  // 身份证名称
        idNo                    // 身份证号
        consignee               // 收货人
        consigneeTel            // 收货人电话
        isErr                   // 是否异常订单
        country                 // 国家
        province                // 省
        city                    // 市
        area                    // 区
        street                  // 街道
        orderTime               // 下单时间
        confirmTime             // 确认时间
        agreedDeliveryTime      // 约定发货时间
        actualDeliveryTime      // 实际发货时间
        confirmReceivyTime      // 确认收货时间
        buyTel                  // 下单手机号
        styleId                 // 样式ID
        merchantCode_sku        // 商家编码-sku
        merchantCode_goods      // 商家编码-商品
        courierNumber           // 快递单号
        customsClearanceSn      // 海淘清关单号
        payId                   // 支付ID
        payType                 // 支付类型
        isFree                  // 是否抽奖或0元试用
        afterSalesState         // 售后状态
        merchantRemark          // 商家备注
        buyMsg                  // 买家留言
        isOfflineStore          // 是否门店自提
        ctime                   // 创建时间
        utime                   // 修改时间
        remark                  // 备注
### 2.2 pay.csv
    支付信息表：
    包含字段：
        id                      // ID
        type                    // 支付类型     
        orderID                 // 订单ID
        bankCard                // 支付银行卡
        cardType                // 支付银行卡类型
        ctime                   // 创建时间
        utime                   // 修改时间
        remark                  // 备注
### 2.2 mapreduce-paper-part0.txt
    MapReduce论文的文章摘要
### 2.2 mapreduce-paper-part1.txt
    MapReduce论文的文章简介