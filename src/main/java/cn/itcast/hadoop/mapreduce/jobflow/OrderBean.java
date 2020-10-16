package cn.itcast.hadoop.mapreduce.jobflow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * PDD订单POJO
 * @ClassName OrderBean
 * @Description
 * @Created by MengYao
 * @Date 2020/10/15 18:27
 * @Version V1.0
 */
public class OrderBean implements Writable {

    private long id;
    private String orderSn;                // 订单编号
    private String state;                  // 订单状态
    private long goodsId;                  // 商品ID
    private String goodsName;              // 商品名称
    private String goodsAttr;              // 商品属性
    private int goodsCount;                // 商品数量
    private double goodsTotalAmount;       // 商品总价
    private double shopDiscount;           // 店铺优惠
    private double platfromDiscount;       // 平台优惠
    private double expressAmount;          // 快递费
    private String expressType;            // 快递类型
    private String expressName;            // 快递名称
    private double SMAZF;                  // 上门安装费
    private double SHRHF;                  // 送货入户费
    private double SHRHAZF;                // 送货入户并安装费
    private double actualAmount;           // 实收金额
    private String idName;                 // 身份证名称
    private String idNo;                   // 身份证号
    private String consignee;              // 收货人
    private String consigneeTel;           // 收货人电话
    private int isErr;                     // 是否异常订单
    private String country;                // 国家
    private String province;               // 省
    private String city;                   // 市
    private String area;                   // 区/县
    private String street;                 // 街道
    private String orderTime;              // 下单时间
    private String confirmTime;            // 确认时间
    private String agreedDeliveryTime;     // 约定发货时间
    private String actualDeliveryTime;     // 实际发货时间
    private String confirmReceivyTime;     // 确认收货时间
    private String buyTel;                 // 下单手机号
    private String styleId;                // 样式ID
    private String merchantCode_sku;       // 商家编码-SKU
    private String merchantCode_goods;     // 商家编码-商品
    private String courierNumber;          // 快递单号
    private String customsClearanceSn;     // 海淘清关单号
    private String payId;                  // 支付ID
    private String payType;                // 支付类型
    private int isFree;                    // 是否抽奖或0元试用
    private int afterSalesState;           // 售后状态
    private String merchantRemark;         // 商家备注
    private String buyMsg;                 // 买家留言
    private int isOfflineStore;            // 是否门店自提
    private String ctime;                  // 创建时间
    private String utime;                  // 修改时间
    private String remark;                 // 备注

    public OrderBean() {
    }

    public OrderBean(long id, String orderSn, String state, long goodsId, String goodsName, String goodsAttr, int goodsCount, double goodsTotalAmount, double shopDiscount, double platfromDiscount, double expressAmount, String expressType, String expressName, double SMAZF, double SHRHF, double SHRHAZF, double actualAmount, String idName, String idNo, String consignee, String consigneeTel, int isErr, String country, String province, String city, String area, String street, String orderTime, String confirmTime, String agreedDeliveryTime, String actualDeliveryTime, String confirmReceivyTime, String buyTel, String styleId, String merchantCode_sku, String merchantCode_goods, String courierNumber, String customsClearanceSn, String payId, String payType, int isFree, int afterSalesState, String merchantRemark, String buyMsg, int isOfflineStore, String ctime, String utime, String remark) {
        this.id = id;
        this.orderSn = orderSn;
        this.state = state;
        this.goodsId = goodsId;
        this.goodsName = goodsName;
        this.goodsAttr = goodsAttr;
        this.goodsCount = goodsCount;
        this.goodsTotalAmount = goodsTotalAmount;
        this.shopDiscount = shopDiscount;
        this.platfromDiscount = platfromDiscount;
        this.expressAmount = expressAmount;
        this.expressType = expressType;
        this.expressName = expressName;
        this.SMAZF = SMAZF;
        this.SHRHF = SHRHF;
        this.SHRHAZF = SHRHAZF;
        this.actualAmount = actualAmount;
        this.idName = idName;
        this.idNo = idNo;
        this.consignee = consignee;
        this.consigneeTel = consigneeTel;
        this.isErr = isErr;
        this.country = country;
        this.province = province;
        this.city = city;
        this.area = area;
        this.street = street;
        this.orderTime = orderTime;
        this.confirmTime = confirmTime;
        this.agreedDeliveryTime = agreedDeliveryTime;
        this.actualDeliveryTime = actualDeliveryTime;
        this.confirmReceivyTime = confirmReceivyTime;
        this.buyTel = buyTel;
        this.styleId = styleId;
        this.merchantCode_sku = merchantCode_sku;
        this.merchantCode_goods = merchantCode_goods;
        this.courierNumber = courierNumber;
        this.customsClearanceSn = customsClearanceSn;
        this.payId = payId;
        this.payType = payType;
        this.isFree = isFree;
        this.afterSalesState = afterSalesState;
        this.merchantRemark = merchantRemark;
        this.buyMsg = buyMsg;
        this.isOfflineStore = isOfflineStore;
        this.ctime = ctime;
        this.utime = utime;
        this.remark = remark;
    }

    public OrderBean(String id, String orderSn, String state, String goodsId, String goodsName, String goodsAttr, String goodsCount, String goodsTotalAmount, String shopDiscount, String platfromDiscount, String expressAmount, String expressType, String expressName, String SMAZF, String SHRHF, String SHRHAZF, String actualAmount, String idName, String idNo, String consignee, String consigneeTel, String isErr, String country, String province, String city, String area, String street, String orderTime, String confirmTime, String agreedDeliveryTime, String actualDeliveryTime, String confirmReceivyTime, String buyTel, String styleId, String merchantCode_sku, String merchantCode_goods, String courierNumber, String customsClearanceSn, String payId, String payType, String isFree, String afterSalesState, String merchantRemark, String buyMsg, String isOfflineStore, String ctime, String utime, String remark) {
        this.id = Long.parseLong(id);
        this.orderSn = orderSn;
        this.state = state;
        this.goodsId = Long.parseLong(goodsId);
        this.goodsName = goodsName;
        this.goodsAttr = goodsAttr;
        this.goodsCount = Integer.parseInt(goodsCount);
        this.goodsTotalAmount = Double.parseDouble(goodsTotalAmount);
        this.shopDiscount = Double.parseDouble(shopDiscount);
        this.platfromDiscount = Double.parseDouble(platfromDiscount);
        this.expressAmount = Double.parseDouble(expressAmount);
        this.expressType = expressType;
        this.expressName = expressName;
        this.SMAZF = Double.parseDouble(SMAZF);
        this.SHRHF = Double.parseDouble(SHRHF);
        this.SHRHAZF = Double.parseDouble(SHRHAZF);
        this.actualAmount = Double.parseDouble(actualAmount);
        this.idName = idName;
        this.idNo = idNo;
        this.consignee = consignee;
        this.consigneeTel = consigneeTel;
        this.isErr = Integer.parseInt(isErr);
        this.country = country;
        this.province = province;
        this.city = city;
        this.area = area;
        this.street = street;
        this.orderTime = orderTime;
        this.confirmTime = confirmTime;
        this.agreedDeliveryTime = agreedDeliveryTime;
        this.actualDeliveryTime = actualDeliveryTime;
        this.confirmReceivyTime = confirmReceivyTime;
        this.buyTel = buyTel;
        this.styleId = styleId;
        this.merchantCode_sku = merchantCode_sku;
        this.merchantCode_goods = merchantCode_goods;
        this.courierNumber = courierNumber;
        this.customsClearanceSn = customsClearanceSn;
        this.payId = payId;
        this.payType = payType;
        this.isFree = Integer.parseInt(isFree);
        this.afterSalesState = Integer.parseInt(afterSalesState);
        this.merchantRemark = merchantRemark;
        this.buyMsg = buyMsg;
        this.isOfflineStore = Integer.parseInt(isOfflineStore);
        this.ctime = ctime;
        this.utime = utime;
        this.remark = remark;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getOrderSn() {
        return orderSn;
    }

    public void setOrderSn(String orderSn) {
        this.orderSn = orderSn;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public long getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(long goodsId) {
        this.goodsId = goodsId;
    }

    public String getGoodsName() {
        return goodsName;
    }

    public void setGoodsName(String goodsName) {
        this.goodsName = goodsName;
    }

    public String getGoodsAttr() {
        return goodsAttr;
    }

    public void setGoodsAttr(String goodsAttr) {
        this.goodsAttr = goodsAttr;
    }

    public int getGoodsCount() {
        return goodsCount;
    }

    public void setGoodsCount(int goodsCount) {
        this.goodsCount = goodsCount;
    }

    public double getGoodsTotalAmount() {
        return goodsTotalAmount;
    }

    public void setGoodsTotalAmount(double goodsTotalAmount) {
        this.goodsTotalAmount = goodsTotalAmount;
    }

    public double getShopDiscount() {
        return shopDiscount;
    }

    public void setShopDiscount(double shopDiscount) {
        this.shopDiscount = shopDiscount;
    }

    public double getPlatfromDiscount() {
        return platfromDiscount;
    }

    public void setPlatfromDiscount(double platfromDiscount) {
        this.platfromDiscount = platfromDiscount;
    }

    public double getExpressAmount() {
        return expressAmount;
    }

    public void setExpressAmount(double expressAmount) {
        this.expressAmount = expressAmount;
    }

    public String getExpressType() {
        return expressType;
    }

    public void setExpressType(String expressType) {
        this.expressType = expressType;
    }

    public String getExpressName() {
        return expressName;
    }

    public void setExpressName(String expressName) {
        this.expressName = expressName;
    }

    public double getSMAZF() {
        return SMAZF;
    }

    public void setSMAZF(double SMAZF) {
        this.SMAZF = SMAZF;
    }

    public double getSHRHF() {
        return SHRHF;
    }

    public void setSHRHF(double SHRHF) {
        this.SHRHF = SHRHF;
    }

    public double getSHRHAZF() {
        return SHRHAZF;
    }

    public void setSHRHAZF(double SHRHAZF) {
        this.SHRHAZF = SHRHAZF;
    }

    public double getActualAmount() {
        return actualAmount;
    }

    public void setActualAmount(double actualAmount) {
        this.actualAmount = actualAmount;
    }

    public String getIdName() {
        return idName;
    }

    public void setIdName(String idName) {
        this.idName = idName;
    }

    public String getIdNo() {
        return idNo;
    }

    public void setIdNo(String idNo) {
        this.idNo = idNo;
    }

    public String getConsignee() {
        return consignee;
    }

    public void setConsignee(String consignee) {
        this.consignee = consignee;
    }

    public String getConsigneeTel() {
        return consigneeTel;
    }

    public void setConsigneeTel(String consigneeTel) {
        this.consigneeTel = consigneeTel;
    }

    public int getIsErr() {
        return isErr;
    }

    public void setIsErr(int isErr) {
        this.isErr = isErr;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public String getConfirmTime() {
        return confirmTime;
    }

    public void setConfirmTime(String confirmTime) {
        this.confirmTime = confirmTime;
    }

    public String getAgreedDeliveryTime() {
        return agreedDeliveryTime;
    }

    public void setAgreedDeliveryTime(String agreedDeliveryTime) {
        this.agreedDeliveryTime = agreedDeliveryTime;
    }

    public String getActualDeliveryTime() {
        return actualDeliveryTime;
    }

    public void setActualDeliveryTime(String actualDeliveryTime) {
        this.actualDeliveryTime = actualDeliveryTime;
    }

    public String getConfirmReceivyTime() {
        return confirmReceivyTime;
    }

    public void setConfirmReceivyTime(String confirmReceivyTime) {
        this.confirmReceivyTime = confirmReceivyTime;
    }

    public String getBuyTel() {
        return buyTel;
    }

    public void setBuyTel(String buyTel) {
        this.buyTel = buyTel;
    }

    public String getStyleId() {
        return styleId;
    }

    public void setStyleId(String styleId) {
        this.styleId = styleId;
    }

    public String getMerchantCode_sku() {
        return merchantCode_sku;
    }

    public void setMerchantCode_sku(String merchantCode_sku) {
        this.merchantCode_sku = merchantCode_sku;
    }

    public String getMerchantCode_goods() {
        return merchantCode_goods;
    }

    public void setMerchantCode_goods(String merchantCode_goods) {
        this.merchantCode_goods = merchantCode_goods;
    }

    public String getCourierNumber() {
        return courierNumber;
    }

    public void setCourierNumber(String courierNumber) {
        this.courierNumber = courierNumber;
    }

    public String getCustomsClearanceSn() {
        return customsClearanceSn;
    }

    public void setCustomsClearanceSn(String customsClearanceSn) {
        this.customsClearanceSn = customsClearanceSn;
    }

    public String getPayId() {
        return payId;
    }

    public void setPayId(String payId) {
        this.payId = payId;
    }

    public String getPayType() {
        return payType;
    }

    public void setPayType(String payType) {
        this.payType = payType;
    }

    public int getIsFree() {
        return isFree;
    }

    public void setIsFree(int isFree) {
        this.isFree = isFree;
    }

    public int getAfterSalesState() {
        return afterSalesState;
    }

    public void setAfterSalesState(int afterSalesState) {
        this.afterSalesState = afterSalesState;
    }

    public String getMerchantRemark() {
        return merchantRemark;
    }

    public void setMerchantRemark(String merchantRemark) {
        this.merchantRemark = merchantRemark;
    }

    public String getBuyMsg() {
        return buyMsg;
    }

    public void setBuyMsg(String buyMsg) {
        this.buyMsg = buyMsg;
    }

    public int getIsOfflineStore() {
        return isOfflineStore;
    }

    public void setIsOfflineStore(int isOfflineStore) {
        this.isOfflineStore = isOfflineStore;
    }

    public String getCtime() {
        return ctime;
    }

    public void setCtime(String ctime) {
        this.ctime = ctime;
    }

    public String getUtime() {
        return utime;
    }

    public void setUtime(String utime) {
        this.utime = utime;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    @Override
    public String toString() {
        return
                id + "\t" +
                orderSn + "\t" +
                state + "\t" +
                goodsId + "\t" +
                goodsName + "\t" +
                goodsAttr + "\t" +
                goodsCount + "\t" +
                goodsTotalAmount + "\t" +
                shopDiscount + "\t" +
                platfromDiscount + "\t" +
                expressAmount + "\t" +
                expressType + "\t" +
                expressName + "\t" +
                SMAZF + "\t" +
                SHRHF + "\t" +
                SHRHAZF + "\t" +
                actualAmount + "\t" +
                idName + "\t" +
                idNo + "\t" +
                consignee + "\t" +
                consigneeTel + "\t" +
                isErr + "\t" +
                country + "\t" +
                province + "\t" +
                city + "\t" +
                area + "\t" +
                street + "\t" +
                orderTime + "\t" +
                confirmTime + "\t" +
                agreedDeliveryTime + "\t" +
                actualDeliveryTime + "\t" +
                confirmReceivyTime + "\t" +
                buyTel + "\t" +
                styleId + "\t" +
                merchantCode_sku + "\t" +
                merchantCode_goods + "\t" +
                courierNumber + "\t" +
                customsClearanceSn + "\t" +
                payId + "\t" +
                payType + "\t" +
                isFree + "\t" +
                afterSalesState + "\t" +
                merchantRemark + "\t" +
                buyMsg + "\t" +
                isOfflineStore + "\t" +
                ctime + "\t" +
                utime + "\t" +
                remark;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeUTF(orderSn);
        out.writeUTF(state);
        out.writeLong(goodsId);
        out.writeUTF(goodsName);
        out.writeUTF(goodsAttr);
        out.writeInt(goodsCount);
        out.writeDouble(goodsTotalAmount);
        out.writeDouble(shopDiscount);
        out.writeDouble(platfromDiscount);
        out.writeDouble(expressAmount);
        out.writeUTF(expressType);
        out.writeUTF(expressName);
        out.writeDouble(SMAZF);
        out.writeDouble(SHRHF);
        out.writeDouble(SHRHAZF);
        out.writeDouble(actualAmount);
        out.writeUTF(idName);
        out.writeUTF(idNo);
        out.writeUTF(consignee);
        out.writeUTF(consigneeTel);
        out.writeInt(isErr);
        out.writeUTF(country);
        out.writeUTF(province);
        out.writeUTF(city);
        out.writeUTF(area);
        out.writeUTF(street);
        out.writeUTF(orderTime);
        out.writeUTF(confirmTime);
        out.writeUTF(agreedDeliveryTime);
        out.writeUTF(actualDeliveryTime);
        out.writeUTF(confirmReceivyTime);
        out.writeUTF(buyTel);
        out.writeUTF(styleId);
        out.writeUTF(merchantCode_sku);
        out.writeUTF(merchantCode_goods);
        out.writeUTF(courierNumber);
        out.writeUTF(customsClearanceSn);
        out.writeUTF(payId);
        out.writeUTF(payType);
        out.writeInt(isFree);
        out.writeInt(afterSalesState);
        out.writeUTF(merchantRemark);
        out.writeUTF(buyMsg);
        out.writeInt(isOfflineStore);
        out.writeUTF(ctime);
        out.writeUTF(utime);
        out.writeUTF(remark);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.orderSn = in.readUTF();
        this.state = in.readUTF();
        this.goodsId = in.readLong();
        this.goodsName = in.readUTF();
        this.goodsAttr = in.readUTF();
        this.goodsCount = in.readInt();
        this.goodsTotalAmount = in.readDouble();
        this.shopDiscount = in.readDouble();
        this.platfromDiscount = in.readDouble();
        this.expressAmount = in.readDouble();
        this.expressType = in.readUTF();
        this.expressName = in.readUTF();
        this.SMAZF = in.readDouble();
        this.SHRHF = in.readDouble();
        this.SHRHAZF = in.readDouble();
        this.actualAmount = in.readDouble();
        this.idName = in.readUTF();
        this.idNo = in.readUTF();
        this.consignee = in.readUTF();
        this.consigneeTel = in.readUTF();
        this.isErr = in.readInt();
        this.country = in.readUTF();
        this.province = in.readUTF();
        this.city = in.readUTF();
        this.area = in.readUTF();
        this.street = in.readUTF();
        this.orderTime = in.readUTF();
        this.confirmTime = in.readUTF();
        this.agreedDeliveryTime = in.readUTF();
        this.actualDeliveryTime = in.readUTF();
        this.confirmReceivyTime = in.readUTF();
        this.buyTel = in.readUTF();
        this.styleId = in.readUTF();
        this.merchantCode_sku = in.readUTF();
        this.merchantCode_goods = in.readUTF();
        this.courierNumber = in.readUTF();
        this.customsClearanceSn = in.readUTF();
        this.payId = in.readUTF();
        this.payType = in.readUTF();
        this.isFree = in.readInt();
        this.afterSalesState = in.readInt();
        this.merchantRemark = in.readUTF();
        this.buyMsg = in.readUTF();
        this.isOfflineStore = in.readInt();
        this.ctime = in.readUTF();
        this.utime = in.readUTF();
        this.remark = in.readUTF();
    }
}
