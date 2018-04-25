# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import functools

from pyspark.ml.pipeline import Estimator, Model
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.param.shared import *
import pyspark.sql.functions as F
from pyspark.sql import Row

from params import HasOutputColsPrefix, HasFieldValues, FillMode


class StringDisassembleModel(Model, HasInputCol, HasOutputCols, 
                             HasFieldValues, FillMode, 
                             DefaultParamsReadable, DefaultParamsWritable):
    
    def getMode(self):
        values = self.getFieldValues()
        return None if len(values) == 0 else values[0]
    
    def _transform(self, dataset):
        x = self.getInputCol()
        fields = self.getOutputCols()
        values = self.getFieldValues()
        fill_mode = self.getFillMode()
        mode = self.getMode()
        
        new_df = dataset
        for f, v in zip(fields, values):
            null_cond = None
            if fill_mode:
                null_cond = (F.lit(mode) == F.lit(v)).cast('double')
            else:
                null_cond = 0
        
            new_df = new_df.withColumn(f, F.when(F.isnull(x), null_cond) \
                    .otherwise((F.col(x) == F.lit(v)).cast('double')))
        
        return new_df


class StringDisassembler(Estimator, HasInputCol, HasOutputColsPrefix, FillMode):
    
    def get_values(self, dataset):
        x = self.getInputCol()
        # values ordered by count (mode)
        values = dataset \
            .where('{} is not null'.format(x)) \
            .groupBy(x) \
            .agg(F.count('*').alias('count')) \
            .orderBy(F.desc('count')) \
            .rdd.map(lambda r: r[x]) \
            .collect()
            
        return values
    
    def get_fields(self, values):
        x = self.getInputCol()
        prefix = self.getOutputColsPrefix()
        return ['{}_{}_{}'.format(prefix, x, v) for v in values]
    
    def _fit(self, dataset):
        x = self.getInputCol()
        values = self.get_values(dataset)
        fields = self.get_fields(values)
        model = StringDisassembleModel() \
            .setInputCol(x) \
            .setOutputCols(fields) \
            .setFieldValues(values) \
            .setFillMode(self.getFillMode())

        return model