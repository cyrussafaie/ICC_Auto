

#######################
#######################
# this code is perfect for reading Excel file
#######################
#######################
library(openxlsx)
library(stringr)
library(DBI)
library(rJava)
library(RJDBC)
library(dplyr)

# m <- read.xlsx("AllBuyers_FI950R01_02122017_12252789_2.xlsx",detectDates = T)
# dim(m)
# head(m)
# 
# loc="//2sxprinffs01/Group/Replenishment/NetWatcherReports/FI950R01/AllBuyers_FI950R01_02122017_12252789.xlsx"
# m2 <- read.xlsx(loc,detectDates = T)
# dim(m2)
# head(m2)

#######################
#######################
# follwoing script finds the most recent icc report and load it here
#######################
#######################
filenames <-list.files("//2sxprinffs01/Group/Replenishment/NetWatcherReports/FI950R01", pattern="*.xlsx", full.names=TRUE) #store all file name
currentyear <- substr(filenames,86,89) 
max.ldf=max(as.numeric(currentyear)) # find the msot recent year

new.pattern=paste('*.',max.ldf,'.*.xlsx',sep = "")
filenames <-list.files("//2sxprinffs01/Group/Replenishment/NetWatcherReports/FI950R01", pattern=new.pattern, full.names=TRUE) #store current year file name
file.extension.name <- substr(filenames,82,89)
max.date=max(as.numeric(file.extension.name)) # find the most recent file

require(stringr)
max.date=str_pad(max.date, 8, pad = "0") # add 0 in the front if needed
new.pattern2=paste('*.','_',max.date,'.*.xlsx',sep = "")
filenames_toUpload <-list.files("//2sxprinffs01/Group/Replenishment/NetWatcherReports/FI950R01", pattern=new.pattern2, full.names=TRUE) #store current year file name

icc_cmod <- read.xlsx(filenames_toUpload,detectDates = T) # read the most recent ICC file
icc_cmod <- icc_cmod[!duplicated(icc_cmod), ] # duplicates removed
#dim(icc_cmod)
icc_cmod$DATE=as.Date(icc_cmod$DATE,origin = "1899-12-30")

#######################
#######################
# getting the PIM and Division name data from Precima DB
#######################
#######################
require(RJDBC)
drv <- JDBC(driverClass="org.netezza.Driver", classPath = "C://JDBC//nzjdbc3.jar", "'")
con <- dbConnect(drv, "jdbc:netezza://159.127.86.216:5480//pprcmusf01", "hsafaie", "pass")

a="select div_nm, div_nbr,rgn_nm, zone_nm   
from prcmuser_usf_division 
group by div_nm, div_nbr,rgn_nm, zone_nm"

divs=dbGetQuery(con, a)


###unique product in the list
prod_nbrs_unique=unique(icc_cmod$PRODUCT.NUMBER)
prod_nbrs_unique_fromatted=paste(toString(prod_nbrs_unique),collapse=',')
prod_nbrs_unique_fromatted=gsub(" ", "", prod_nbrs_unique_fromatted, fixed = TRUE)
prod_nbrs_unique_fromatted=gsub(",","','",prod_nbrs_unique_fromatted)
prod_nbrs_unique_fromatted=paste("('",prod_nbrs_unique_fromatted,"')")
prod_nbrs_unique_fromatted=gsub(" ", "", prod_nbrs_unique_fromatted, fixed = TRUE)

# this view only looks at 
b=paste("select prod_nbr, pim_cls_desc, pim_ctgry_desc, pim_grp_desc, pim_mrch_ctgry_nm from prcmuser_usf_product where prod_nbr in ", prod_nbrs_unique_fromatted, " group by prod_nbr, pim_cls_desc, pim_ctgry_desc, pim_grp_desc, pim_mrch_ctgry_nm",sep=" ")

prods=dbGetQuery(con, b)
#object.size(prods)

dbDisconnect(con)

#######################
#######################
# forward buy data
#######################
#######################
forward_buy <- read.xlsx("//m1xv03sfs01/group/Marketing/Pricing/Remote Desktop/Sunday Reval/Part 1 (CMOD Monarch)/Forward Buy Combo by Month.xlsx",detectDates = T) # read the most recent ICC file
forward_buy <- forward_buy[!duplicated(forward_buy), ] # duplicates removed
dim(forward_buy)
head(forward_buy)
#forward_buy1=forward_buy[,1]

forward_buy$X=paste(forward_buy$Combo,forward_buy$Month.Number,sep = "")
forward_buy$X=as.numeric(forward_buy$X)
forward_buy1=forward_buy


sorted <- forward_buy1 %>% 
          arrange(Combo, -X) %>%
          group_by(Combo) %>%
          mutate(rank=row_number())
forward_buy1=sorted[sorted$rank==1,]
forward_buy=forward_buy1

# forward_buy2=forward_buy$Combo
# forward_buy[forward_buy2 %in% unique(forward_buy2[duplicated(forward_buy2)])]
# 
# 
# forward_buy[duplicated(forward_buy[,c(1,3)])]
# unique(forward_buy$Month.Number)

############ relevent files to be merged are forward_buy, divs, prods, and icc_cmod 

# dim(forward_buy)
# dim(divs)
# dim(prods)
# dim(icc_cmod)
# "//m1xv03sfs01/group/Marketing/Pricing/Remote Desktop/Sunday Reval/Part 1 (CMOD Monarch)
# head(forward_buy)
# names(divs)
# names(prods)
# names(icc_cmod)

##### merging data with division
icc_cmod$ID=1:nrow(icc_cmod) # add order
final_icc=merge(x=icc_cmod[,c("ID","DISTRICT","ACCOUNTING.WEEK","PRODUCT.NUMBER","DESCRIPTION"
            ,"DATE","SOURCE","PO.NUMBER","NEW.UNIT.COST","PRIOR.UNIT.COST"
            ,"UNIT.COST.CHANGE","INVENTORY.VALUE.CHANGE","BUYER","BUYER.NAME"
            ,"VENDOR","VENDOR.NAME")],y=divs, by.x=c("DISTRICT"),by.y=c("div_nbr")
            ,all.x=TRUE)[,c("ID","DISTRICT","rgn_nm","div_nm","ACCOUNTING.WEEK","PRODUCT.NUMBER","DESCRIPTION"
                            ,"DATE","SOURCE","PO.NUMBER","NEW.UNIT.COST","PRIOR.UNIT.COST"
                            ,"UNIT.COST.CHANGE","INVENTORY.VALUE.CHANGE","BUYER","BUYER.NAME"
                            ,"VENDOR","VENDOR.NAME","zone_nm")]

# names(final_icc)
# names(prods)
##### merging data with product
final_icc=merge(x=final_icc, y=prods, by.x = c("PRODUCT.NUMBER")
                ,by.y = c("prod_nbr"),all.x=TRUE)[ ,c("ID","DISTRICT","rgn_nm","div_nm","ACCOUNTING.WEEK","PRODUCT.NUMBER","DESCRIPTION"
          ,"pim_cls_desc","pim_ctgry_desc","pim_grp_desc","DATE","SOURCE","PO.NUMBER","NEW.UNIT.COST","PRIOR.UNIT.COST"
          ,"UNIT.COST.CHANGE","INVENTORY.VALUE.CHANGE","BUYER","BUYER.NAME"
          ,"VENDOR","VENDOR.NAME","zone_nm")]

# dim(final_icc)                      
# 
# head(final_icc)
#      
# head(final_icc)

Combo=paste0(final_icc$DISTRICT,final_icc$PRODUCT.NUMBER)

final_icc=cbind(final_icc,Combo)[ ,c("ID","DISTRICT","rgn_nm","div_nm","ACCOUNTING.WEEK","Combo","PRODUCT.NUMBER","DESCRIPTION"
                                     ,"pim_cls_desc","pim_ctgry_desc","pim_grp_desc","DATE","SOURCE","PO.NUMBER","NEW.UNIT.COST","PRIOR.UNIT.COST"
                                     ,"UNIT.COST.CHANGE","INVENTORY.VALUE.CHANGE","BUYER","BUYER.NAME"
                                     ,"VENDOR","VENDOR.NAME","zone_nm")]

final_icc=merge(x=final_icc,y=forward_buy, by="Combo",all.x=TRUE)[,c("ID","DISTRICT","rgn_nm","div_nm","ACCOUNTING.WEEK","Combo","Forward.Buy","PRODUCT.NUMBER","DESCRIPTION"
                                                                    ,"pim_cls_desc","pim_ctgry_desc","pim_grp_desc","DATE","SOURCE","PO.NUMBER","NEW.UNIT.COST","PRIOR.UNIT.COST"
                                                                    ,"UNIT.COST.CHANGE","INVENTORY.VALUE.CHANGE","BUYER","BUYER.NAME"
                                                                    ,"VENDOR","VENDOR.NAME","zone_nm")]

final_icc=final_icc[order(final_icc$ID),]
final_icc=final_icc[,-1]

final_icc$ACCOUNTING.WEEK <- substr(final_icc$ACCOUNTING.WEEK,1,7) # fixing fiscal week text

#unique(final_icc$SOURCE)
final_icc$SOURCE[is.na(final_icc$SOURCE)] <- "Cost Adjst" #replacing source=Null with Cost Adjst

#write.csv(final_icc,paste("//m1xv03sfs01/group/Marketing/Pricing/Remote Desktop/Sunday Reval/Part 1 (CMOD Monarch)/CMOD Reports/current Year CMOD ",icc_cmod$RUN_DATE[1],".csv",sep = ""),row.names = F,na="")

#dim(final_icc)
this=paste("C:/Users/e026026/Documents/Analysis 2017/ICC automation/ICC_Automation/current Year CMOD",icc_cmod$RUN_DATE[1],".csv",sep = "")
write.csv(final_icc,this,row.names = F,na="")

#write.csv(final_icc,this, row.names = F,na = "")
#unique(final_icc$SOURCE)

#getwd()
