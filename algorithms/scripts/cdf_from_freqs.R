#!/usr/bin/Rscript

library(ggplot2)
#require(scales)
args = commandArgs(trailingOnly=TRUE)

raw <- read.table(args[1], header=F, sep=" ")
recordsPerKey <- sort(raw$V2)
probab <- (cumsum(recordsPerKey) / sum(recordsPerKey))

print ("data read")

cdfdf <- data.frame(recordsPerKey, probab)
gcdf <- ggplot (data=cdfdf, aes(x=cdfdf$recordsPerKey, y=cdfdf$probab)) +
   stat_binhex(bins = 100) +
   theme_minimal(base_size = 18) +
   xlab ("x = Records per Key") +
   ylab ("P[X <= x]")

iprobab <- 1 - probab
ccdfdf <- data.frame(recordsPerKey, iprobab)
gccdf <- ggplot (data=ccdfdf, aes(x=ccdfdf$recordsPerKey, y=ccdfdf$iprobab)) +
   stat_binhex(bins = 100) +
   theme_minimal (base_size = 18) +
   xlab ("x = Records per Key") +
   ylab ("P[X > x]")

ggsave(filename=paste(args[1], "cdf", "pdf", sep="."), plot=gcdf)
ggsave(filename=paste(args[1], "ccdf", "pdf", sep="."), plot=gccdf)
