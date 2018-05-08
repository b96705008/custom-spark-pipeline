# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from pyspark.ml.pipeline import Estimator
from pyspark.ml.param.shared import *
import pyspark.sql.functions as F

from value_generator import *
from impute_model import TykuoImputerModel


class HasImputeStrategy(Params):
    
    strategy = Param(Params._dummy(),
                         'strategy', 
                         'strategy to impute value',
                         typeConverter=TypeConverters.toString)
    
    def __init__(self):
        super(HasImputeStrategy, self).__init__()
        self._setDefault(strategy='median')
    
    def setStrategy(self, value):
        return self._set(strategy=value)
    
    def getStrategy(self):
        return self.getOrDefault(self.strategy)


class TykuoImputer(Estimator, HasInputCol, HasOutputCol, HasImputeStrategy):
    
    def get_value_generator(self, strategy='median'):
        if strategy == 'median':
            return MedianGenerator()
        elif strategy == 'mean':
            return MeanGenerator()
        elif strategy == 'mode':
            return ModeGenerator()
        else:
            raise Exception('Strategy should only be median, mean or mode')
    
    def _fit(self, dataset):
        # get value generator
        strategy = self.getStrategy()
        value_gen = self.get_value_generator(strategy)
        
        # compute value to be imputed
        x = self.getInputCol()
        impute_value = value_gen.generate(dataset, x)
        
        # create impute model
        impute_model = TykuoImputerModel() \
            .setInputCol(x) \
            .setOutputCol(self.getOutputCol()) \
            .setImputeValue(impute_value)
            
        return impute_model