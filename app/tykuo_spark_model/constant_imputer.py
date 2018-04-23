# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import *
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from params import HasConstValue


class ConstantImputer(Transformer, HasInputCols, HasOutputCols, HasConstValue, 
                      DefaultParamsReadable, DefaultParamsWritable):
    
    def _transform(self, dataset):
        const_val = self.getConstValue()
        if type(const_val) is dict:
            # fill dict
            return dataset.na.fill(const_val)
        else:
            xs = None
            try:
                xs = self.getInputCols()
            except KeyError as e:
                # fill constant string or number
                return dataset.na.fill(const_val)
            
            ys = xs
            try:
                ys = self.getOutputCols()
            except KeyError as e:
                pass
            
            filled_dataset = dataset
            for x, y in zip(xs, ys):
                filled_dataset = filled_dataset \
                    .withColumn(y, F.when(F.col(x).isNull(), const_val).otherwise(F.col(x)))
            
            return filled_dataset