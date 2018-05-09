# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from pyspark.ml.pipeline import Model
from pyspark.ml.param.shared import *
import pyspark.sql.functions as F


class HasImputeValue(Params):
    
    impute_value = Param(Params._dummy(),
                         'impute_value', 
                         'value imputed to missing column')
    
    def __init__(self):
        super(HasImputeValue, self).__init__()
    
    def setImputeValue(self, value):
        return self._set(impute_value=value)
    
    def getImputeValue(self):
        return self.getOrDefault(self.impute_value)


class TykuoImputerModel(Model, HasInputCol, HasOutputCol, HasImputeValue):
    
    def _transform(self, dataset):
        x = self.getInputCol()
        y = self.getOutputCol()
        impute_value = self.getImputeValue()
        imputed_df = dataset.withColumn(
            y, F.when(F.col(x).isNull(), impute_value).otherwise(F.col(x)))
        
        return imputed_df