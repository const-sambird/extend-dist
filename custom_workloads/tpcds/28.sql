
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 5 and 5+10 
             or ss_coupon_amt between 1922 and 1922+1000
             or ss_wholesale_cost between 56 and 56+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 156 and 156+10
          or ss_coupon_amt between 2924 and 2924+1000
          or ss_wholesale_cost between 46 and 46+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 53 and 53+10
          or ss_coupon_amt between 4476 and 4476+1000
          or ss_wholesale_cost between 16 and 16+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 134 and 134+10
          or ss_coupon_amt between 17933 and 17933+1000
          or ss_wholesale_cost between 34 and 34+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 102 and 102+10
          or ss_coupon_amt between 14934 and 14934+1000
          or ss_wholesale_cost between 60 and 60+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 47 and 47+10
          or ss_coupon_amt between 8397 and 8397+1000
          or ss_wholesale_cost between 70 and 70+20)) B6
limit 100;


