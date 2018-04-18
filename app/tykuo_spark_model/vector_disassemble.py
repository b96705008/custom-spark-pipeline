# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import functools

from pyspark.ml.pipeline import Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.param.shared import *
import pyspark.sql.functions as F
from pyspark.sql import Row

from params import HasOutputColsPrefix


class VectorDisassembler(Transformer, HasInputCol, HasOutputCols, 
    DefaultParamsReadable, DefaultParamsWritable):
    
    def get_ordered_columns(self, cols):
        ys = self.getOutputCols()
        return cols + [y for y in ys if y not in cols]
    
    @staticmethod
    def disassemble(vec_col, output_cols, row):
        vector = row[vec_col]
        values = map(lambda v: float(v), vector)
        vec_value_dict = dict(zip(output_cols, values))
        row_dict = row.asDict()
        row_dict.update(vec_value_dict)
        return Row(**row_dict)
    
    def _transform(self, dataset):
        x = self.getInputCol()
        ys = self.getOutputCols()
        disamb_func = functools.partial(self.disassemble, x, ys)
        final_cols = self.get_ordered_columns(dataset.columns)
        return dataset.rdd \
            .map(disamb_func) \
            .toDF() \
            .select(final_cols)