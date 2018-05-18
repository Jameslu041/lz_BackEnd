
#-----------------日期传参

Args <- commandArgs()
# Args <- c(1,1,1,1,1,20170701)
dateInputChar <- as.character(Args[6])

if(is.na(dateInputChar)){
  message('未输入日期，将使用当前日期统计')
  dateInputChar <- as.character(Sys.Date() - 1, format='%Y%m%d') #chr:'20170901'
}
dateInput <- as.Date(dateInputChar, format='%Y%m%d') #date:'2017-09-01'
if(is.na(dateInput)) stop('输入日期格式有误，请重新输入！')

cat('使用日期为：', dateInputChar, '\n')


#-----------------初始化

library(sparklyr)
library(dplyr)
library(DBI)
config <- spark_config()
config$spark.executor.cores <- 4
config$spark.executor.memory <- "4G"
config$spark.executor.instances <- 5
config$spark.yarn.executor.memoryOverhead <- 2048
Sys.setenv(JAVA_HOME = "/usr/java/jdk1.8.0_77")
sc <- spark_connect(master = "yarn-client", version = "1.6.0",
                    app_name = paste0("beta_chance", unclass(Sys.time())),
                    spark_home = "/opt/cloudera/parcels/CDH-5.7.0-1.cdh5.7.0.p0.45/lib/spark",
                    config = config)

                    
dateInputChar.jiaoyi <- tbl(sc, "ctprod.sys_calendar") %>% 
  filter(zrr == dateInputChar) %>% select(jyr) %>% collect()
dateInputChar.jiaoyi <- dateInputChar.jiaoyi$jyr
dateInput.jiaoyi = as.Date(dateInputChar.jiaoyi, format='%Y%m%d')
cat('最近一次交易日为：', dateInputChar.jiaoyi, '\n')

#--------------
                    
# 找一帮客户
custIDs <- tbl(sc, "ctprod.dim_customer") %>% 
  select(customer_no) %>% distinct()

# 这帮客户dateInputChar.jiaoyi持有的股票
agg_cust_stock <- tbl(sc, "ctprod.agg_cust_stock") %>% 
  # 这里的filter必须用分区字段否则会报错！
  filter(part_date == dateInputChar.jiaoyi) %>%
  select(customer_no, stock_no, stock_name, current_amount) %>%
  inner_join(custIDs, by = 'customer_no')
stockIDs <- agg_cust_stock %>% select(stock_no) %>% distinct()


#------------------β系数&客户风险收益率

# 大盘信息和股票信息
date_gp <- dateInput - 365
date_gp <- as.character(date_gp, format='%Y%m%d')

dapan <- tbl(sc, "ctprod.fact_stock_market_summary") %>% 
  filter(market_no == '000001') %>% 
  filter(init_date >= date_gp & init_date <= dateInputChar) %>%
  select(init_date, close, change_rate) %>% arrange(init_date)

gupiao <- tbl(sc, "ctprod.fact_stock_market_detail") %>% 
  select(stock_no, stock_name, init_date, price_change_rate) %>% 
  filter(init_date >= date_gp & init_date <= dateInputChar) %>%
  inner_join(stockIDs, by = 'stock_no') %>%
  arrange(stock_no, init_date)

compare <- gupiao %>% inner_join(dapan, by = 'init_date') %>% 
  filter(!is.na(price_change_rate) & !is.na(change_rate)) %>% 
  arrange(stock_no, init_date)

beta <- compare %>%
  spark_apply(group_by = "stock_no", f = function(g){
    tryCatch({
      g2 = g[order(g$init_date), ]
      coefs = coef(lm(price_change_rate ~ change_rate, data = g2))
      data.frame('stock_name' = g2$stock_name[1],
                 'intercept' = coefs[1], 'beta' = coefs[2])
    }, error = function(e) return(data.frame('stock_name' = g2$stock_name[1],
                                             'intercept' = 0, 'beta' = 1)))
  }, columns = c('stock_name', 'intercept', 'beta'))


cust_beta <- agg_cust_stock %>%
  select(customer_no, stock_no, current_amount) %>%
  inner_join(beta, by = 'stock_no') %>%
  group_by(customer_no) %>%
  summarise(cust_beta = sum(current_amount*beta)/sum(current_amount)) %>% 
  ungroup() 

free_rate <- 0.00
first.price <- dapan %>% filter(init_date == min(init_date)) %>% select(close)
last.price <- dapan %>% filter(init_date == max(init_date)) %>% select(close)
dapan_rate <- as.numeric(collect(last.price)$close / collect(first.price)$close) - 1 #0.02
cust_risk_income_rate <- cust_beta %>% 
  mutate(risk_rate = cust_beta*(dapan_rate-free_rate)+free_rate) 
# 12345条记录

  
  
#-----------------买卖时机

date_st <- dateInput - 365
date_st <- as.character(date_st, format='%Y%m%d')

stocks_mini <- tbl(sc, "ctprod.fact_stock_market_detail") %>%
  select(init_date, stock_no, close_price) %>%
  filter(init_date <= dateInputChar & init_date >= date_st) %>%
  inner_join(stockIDs, by = 'stock_no') #260474*3

PeakAndValley <- stocks_mini %>%
  spark_apply(group_by = "stock_no", f = function(g){
    library(zoo)
    library(wmtsa)
    library(data.table)
    
    findPeakAndValley <- function(dates, price, w=2){
      x = dates
      y = price
      n <- length(y)
      y.smooth = wavShrink(y, thresh.scale = 2) #小波去燥
      y.max <- rollapply(zoo(y.smooth), 2*w+1, max, align="center") #滑动max
      y.min <- rollapply(zoo(y.smooth), 2*w+1, min, align="center") #滑动min
      delta1 <- y.max - y.smooth[-c(1:w, n+1-1:w)]
      delta2 <- y.min - y.smooth[-c(1:w, n+1-1:w)]
      i.max <- which(delta1 <= 0) + w
      i.min <- which(delta2 >= 0) + w
      peaks = data.frame('shape' = 'peak', 'idx' = i.max, 
                         'dates' = x[i.max], 'price' = y[i.max])
      valleys = data.frame('shape' = 'valley', 'idx' = i.min, 
                           'dates' = x[i.min], 'price' = y[i.min])
      pv = rbind(peaks, valleys)
      pv[order(pv$idx),]
    }
    
    filterPeakAndValley <- function(pv, threshold = 0.1){
      n_row = nrow(pv)
      pv$sign = 1
      j = 1
      for(i in 1:n_row){
        if(i>=2 & i<=n_row-1 & pv$sign[i]>0){
          myshape = pv$shape[i]; myprice = pv$price[i]
          j = max(which(pv$sign[j:(i-1)]==1)) + j - 1
          shape0 = pv$shape[j]; price0 = pv$price[j]
          shape1 = pv$shape[i+1]; price1 = pv$price[i+1]
          if(myshape!=shape0 & myshape!=shape1){
            change0 = abs((myprice - price0) / price0)
            change1 = abs((price1 - myprice) / myprice)
            if(change0 < threshold){
              pv$sign[i] = -1
              if(change1 >= threshold) { 
                #左小右大，去左
                if(j>=2){
                  pv$sign[j] = -1
                  j = max(which(pv$sign[1:(j-1)]==1))
                }
              } else {
                #左小右小
                #若左大于右且我是波峰，则去左留右
                if((price0 - price1) * ((myshape=='peak') - 0.5) >= 0){
                  if(j>=2){
                    pv$sign[j] = -1
                    j = max(which(pv$sign[1:(j-1)]==1))
                  }
                } else {
                  pv$sign[i+1] = -1
                }
              }
            }
          } else if(myshape==shape0){
            #这里实际上过滤掉了同是波峰/波谷的情况
            if((myprice - price0) * ((myshape=='peak') - 0.5) >= 0){
              if(j>=2){
                pv$sign[j] = -1
                j = max(which(pv$sign[1:(j-1)]==1))
              }
            } else {
              pv$sign[i] = -1
            }
          } else if(myshape==shape1){
            if((myprice - price1) * ((myshape=='peak') - 0.5) >= 0){
              pv$sign[i+1] = -1 
            } else {
              pv$sign[i] = -1
            }
          } 
        }
      }
      pv = pv[pv$sign>0, c('shape', 'idx', 'dates', 'price')]
      pv$shape[1] = ifelse(pv$shape[2]=='peak', 'valley', 'peak')
      pv
    }
    
    tryCatch({
      #寻找和筛选波峰波谷
      g2 = data.table(g)
      g2 = g2[order(init_date), .(init_date, close_price)]
      res1 = findPeakAndValley(dates = g2$init_date, price = g2$close_price)
      res2 = filterPeakAndValley(res1, threshold = 0.1)
      
      #确定日期范围
      res2$dates = as.Date(res2$dates, format = '%Y%m%d')
      n_row = nrow(res2)
      res2$diff = c(0, res2$dates[2:n_row] - res2$dates[1:(n_row-1)])
      res2$range = ifelse(res2$diff>=4, floor(res2$diff/4), 
                          ifelse(res2$diff==3, 1, 0))
      res2$dates_start = res2$dates - res2$range
      res2$dates_end = c(res2$dates[1:(n_row-1)] + res2$range[2:n_row], res2$dates[n_row])
      
      #输出
      res2$dates = as.character(res2$dates, format = '%Y%m%d')
      res2$dates_start = as.character(res2$dates_start, format = '%Y%m%d') 
      res2$dates_end = as.character(res2$dates_end, format = '%Y%m%d')
      res2[,c('shape', 'idx', 'dates', 'price', 'dates_start', 'dates_end')]
    }, error = function(e) data.frame('shape' = 'null', 'idx' = -1, 'dates' = 'null', 'price' = -1,
                                      'dates_start' = 'null', 'dates_end' = 'null'))

  },
  names = c('shape', 'idx', 'dates', 'price', 'dates_start', 'dates_end')) #10396*7


cust_stock_mini <-  tbl(sc, "ctprod.fact_cust_stock_detail") %>% 
  filter(part_date >= date_st & part_date <= dateInputChar) %>%
  select(init_date, customer_no, stock_no, exchange_type) %>%
  inner_join(custIDs, by = 'customer_no') #67183*5
  
clever_counts <- PeakAndValley %>% 
  inner_join(cust_stock_mini, by = 'stock_no') %>%
  filter((exchange_type=='证券卖出' & shape=='peak' & init_date>=dates_start & init_date<=dates_end) |
           (exchange_type=='证券买入' & shape=='valley' & init_date>=dates_start & init_date<=dates_end)) %>%
  group_by(customer_no) %>% count() %>% 
  rename(clever_num = n)

stockIDs2 <- PeakAndValley %>% select(stock_no) %>% distinct()
total_counts <- cust_stock_mini %>% 
  # 通过join去掉不纳入统计的股票
  inner_join(stockIDs2, by = 'stock_no') %>%
  group_by(customer_no) %>% count() %>% 
  rename(total_num = n)

clever_time <- left_join(total_counts, clever_counts, by = 'customer_no') %>% 
  mutate(clever_num = ifelse(is.na(clever_num), 0, clever_num)) %>%
  mutate(clever_ratio = clever_num / total_num)
# 14823条记录

#-----------------写入hive


res <- custIDs %>% 
  left_join(clever_time, by = 'customer_no') %>%
  left_join(cust_risk_income_rate, by = 'customer_no') %>%
  collect() # 12324*8，这里能否不collect？

res <- res %>% mutate(clever_level = if_else(clever_ratio<0.02, '新手客户', 
                                             if_else(clever_ratio<0.1, '一般客户', 
                                                     if_else(clever_ratio<0.2, '机智客户', '卓越客户')), '买卖时机未知'), 
                      beta_level = if_else(cust_beta<0, '低风险负收益客户', 
                                           if_else(cust_beta<1.3, '低风险策略客户', 
                                                   if_else(cust_beta<1.7, '中等风险策略客户', 
                                                           '高风险策略客户')), '风险偏好未知'))

res <- copy_to(sc, res)
library(DBI)
dbExecute(sc, "drop table if exists tag_stat.beta_chance")
spark_write_table(res, 'tag_stat.beta_chance')




spark_disconnect(sc)
