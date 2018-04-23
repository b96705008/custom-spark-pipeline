# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals


from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import *
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from params import HasConstValue

class ConstantImputer(Transformer, HasConstValue, DefaultParamsReadable, DefaultParamsWritable):
    
    def _transform(self, dataset):
        const_val = self.getConstValue()
        return dataset.na.fill(const_val)