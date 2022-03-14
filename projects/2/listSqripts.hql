create database if not exists checker;
use checker;
drop table hw2_test;
source projects/2/create_test.hql;
describe hw2_test;
select count(id) from hw2_test;
drop table hw2_pred;
source projects/2/create_pred.hql;
describe hw2_pred;
source projects/2/filter_predict.hql;
select count(id) from hw2_pred;
source projects/2/select_out.hql;

