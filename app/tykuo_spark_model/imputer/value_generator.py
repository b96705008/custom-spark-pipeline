# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import pyspark.sql.functions as F


class ValueGenerator(object):
    
    def generate(self, dataset, inputCol):
        raise NotImplementedError


class ModeGenerator(ValueGenerator):
    
    def generate(self, dataset, inputCol):
        impute_val = None
        
        if inputCol is None:
            return impute_val
        
        rows = dataset.where('{} is not null'.format(inputCol)) \
            .groupBy(inputCol) \
            .agg(F.count('*').alias('count')) \
            .orderBy(F.desc('count')) \
            .take(1)
        
        if len(rows) > 0:
            impute_val = rows[0][inputCol]
        
        return impute_val


class MedianGenerator(ValueGenerator):
    
    def generate(self, dataset, inputCol):
        impute_val = None
        
        if inputCol is None:
            return impute_val
        
        impute_val = dataset \
            .approxQuantile(str(inputCol), [0.5], 0.25)
        
        if isinstance(impute_val, list):
            impute_val = impute_val[0]
            
        return impute_val


class MeanGenerator(ValueGenerator):
    
    def generate(self, dataset, inputCol):
        agg_func = 'mean'
        impute_val = None
        
        if inputCol is None:
            return impute_val
        
        impute_val = dataset \
            .agg(F.mean(inputCol).alias(agg_func)) \
            .take(1)
        
        if len(impute_val) > 0:
            impute_val = impute_val[0][agg_func]
            
        return impute_val