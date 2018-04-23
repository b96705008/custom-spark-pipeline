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

from params import HasOutputColsPrefix, HasFieldValues


class StringDisassembleModel(Model, HasInputCol, HasOutputCols, HasFieldValues, 
                             DefaultParamsReadable, DefaultParamsWritable):
    
    @staticmethod
    def disassemble_row(x, fields, values, row):
        new_data = {f: float(row[x]==v) 
                    for f, v in zip(fields, values)}
        data = row.asDict()
        data.update(new_data)
        return Row(**data) 
    
    def _transform(self, dataset):
        x = self.getInputCol()
        fields = self.getOutputCols()
        values = self.getFieldValues()
        cols = dataset.columns + fields
        disassemble_func = functools \
            .partial(self.disassemble_row, x, fields, values)
            
        return dataset.rdd \
            .map(disassemble_func) \
            .toDF() \
            .select(*cols)


class StringDisassembler(Estimator, HasInputCol, HasOutputColsPrefix):
    
    def get_fields(self, values):
        x = self.getInputCol()
        prefix = self.getOutputColsPrefix()
        return ['{}_{}_{}'.format(prefix, x, v) for v in values]
    
    def _fit(self, dataset):
        x = self.getInputCol()
        
        values = dataset.rdd \
            .map(lambda r: r[x]) \
            .distinct() \
            .filter(lambda v: v is not None) \
            .collect()
            
        fields = self.get_fields(values)
        
        model = StringDisassembleModel() \
            .setInputCol(x) \
            .setOutputCols(fields) \
            .setFieldValues(values)

        return model