# Investigation of Charity Commission's data for charities in England and Wales

library(ggplot2)
library(reshape2)

setwd('C:\\Users\\willl\\PycharmProjects\\CharityData\\analysis')
setwd('..')
source('.\\analysis\\_connectscript.R')

# Financial table, raw from DB

ewfinance <- runquery(sql = 'select ew_partb.regno, ew_currentname.name, artype, fyend,
                        inc_leg, inc_end, inc_vol, inc_fr, inc_char, inc_invest, inc_other, inc_total,
                        invest_gain, asset_gain, pension_gain,
                        exp_vol, exp_trade, exp_invest, exp_grant, exp_charble, exp_gov, exp_other, exp_total, exp_support, exp_dep,
                        reserves, asset_open, asset_close, fixed_assets, open_assets, invest_assets, cash_assets, current_assets, credit_1, credit_long, pension_assets, total_assets,
                        funds_end, funds_restrict, funds_unrestrict, funds_total,
                        employees, volunteers
                        from ew_partb
                        join ew_currentname on ew_partb.regno = ew_currentname.regno;')

ewfinance$artype <- factor(ewfinance$artype)
ewfinance$regno <- factor(ewfinance$regno)
ewfinance$name <- factor(ewfinance$name)

summary(ewfinance)
str(ewfinance)

# Split into AR year v numerics matrices, one per regno, in a list
# Takes AR entry by latest account submit date

# Create matrix schema to load each charity into

rows <- colnames(ewfinance[5:ncol(ewfinance)])
cols <- levels(ewfinance$artype)
mat_schema <- matrix(nrow = length(rows), 
                     ncol = length(cols), 
                     dimnames = list(rows, 
                                     cols))
rm(rows, cols)

# Split, transform, conform to schema

charities <- split(ewfinance[ , 3:ncol(ewfinance)], ewfinance$regno)
charities <- lapply(charities, 
                    function(x){
                      x <- x[rev(order(x$fyend)), ]
                      x <- x[!duplicated(x$artype), ]
                      x <- x[order(x$fyend), ]
                      x$fyend <- NULL
                      rownames(x) <- x$artype
                      x$artype <- NULL
                      t(x)
                    })
charities <- lapply(charities,
                    function(x){
                      sch <- mat_schema
                      sch[ , colnames(x)] <- x[ , colnames(x)]
                      x <- sch
                    })
rm(mat_schema)

# Analysis

# Correlation between variables

corrmatrix <- lapply(charities, 
                     function(x) {
                       round(cor(t(x), use = 'pairwise.complete.obs'), 2)  
                       })

corrmatrix <- apply(simplify2array(corrmatrix), 1:2, mean, na.rm = TRUE)
corrmatrix <- round(corrmatrix, 3)

# Plot correlation matrix

get_lower_tri <- function(x){
  x[upper.tri(x, diag = TRUE)] <- NA
  return(x)
}

cor_lower_tri <- get_lower_tri(corrmatrix)
melted_corrmatrix <- melt(cor_lower_tri)
rm(get_lower_tri, cor_lower_tri)

ggplot(data = melted_corrmatrix,
       aes(x=Var1, y=Var2, fill=value)) + 
  geom_tile(colour = 'white') +
  scale_fill_gradient2(low = 'blue', high = 'red', mid = 'white', 
                       midpoint = 0, limit = c(-1, 1),
                       na.value = 'lightgrey') +
  theme_minimal() + 
  theme(axis.text.x = element_text(angle = 90, vjust = 0.2, 
                                   size = 7, hjust = 1),
        axis.text.y = element_text(size = 7)) +
  coord_fixed()

# TODO: Throw inc/exp relationship into the mix?

# How much more/less than income was expenditure? >1 means spent more than came in

exp_inc <- t(sapply(charities, apply, 2, function(x){ round(x['exp_total']/x['inc_total'], 2) }))
head(exp_inc)
