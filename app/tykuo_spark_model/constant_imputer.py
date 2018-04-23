# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals


from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import *
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from params import HasConstValue


class ConstantImputer(Transformer, HasInputCols, HasConstValue, 
    DefaultParamsReadable, DefaultParamsWritable):
    
    def _transform(self, dataset):
        const_val = self.getConstValue()
        try:
            xs = self.getInputCols()
            if type(const_val) is dict:
                raise Exception('Multiple fields can only be filled with single value.')
            val_dict = {x: const_val for x in xs}
            return dataset.na.fill(val_dict)
        except KeyError as e:
            return dataset.na.fill(const_val)