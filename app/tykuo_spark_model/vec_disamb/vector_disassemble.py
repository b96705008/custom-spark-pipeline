# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import *
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

class HasOutputCols(Params):

    outputCols = Param(Params._dummy(), "outputCols", 
                         "output columns",
                         typeConverter=TypeConverters.toList)

    def __init__(self):
        super(HasOutputCols, self).__init__()
        
    def setOutputCols(self, value):
        return self._set(outputCols=value)

    def getOutputCols(self):
        return self.getOrDefault(self.outputCols)


class VectorDisassembler(Transformer, HasInputCol, HasOutputCols):
    
    def _transform(self, dataset):
        x = self.getInputCol()  
        
        rows = dataset.select(x).take(1)
        if len(rows) == 0:
            return dataset
        
        vec_size = rows[0][x].size
        ys = None
        try:
            ys = self.getOutputCols()
        except:
            ys = map(lambda i: '{}_{}'.format(x, i), range(vec_size))
        
        vec_disamb_data = dataset
        get_element = F.udf(lambda f, i: float(f[i]), DoubleType())
        for i, y in enumerate(ys):
            vec_disamb_data = vec_disamb_data \
                .withColumn(y, get_element(x, F.lit(i)))
    
        return vec_disamb_data