# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from pyspark.ml.param.shared import *


class HasOutputColsPrefix(Params):

    output_prefix = Param(Params._dummy(), "output_prefix", 
                         "prefix for every output column name",
                         typeConverter=TypeConverters.toString)

    def __init__(self):
        super(HasOutputColsPrefix, self).__init__()
        self._setDefault(output_prefix='is')
    
    def setOutputColsPrefix(self, value):
        return self._set(output_prefix=value)

    def getOutputColsPrefix(self):
        return self.getOrDefault(self.output_prefix)


class HasFieldValues(Params):

    values = Param(Params._dummy(), "values", 
                         "all possible values for a field",
                         typeConverter=TypeConverters.toList)
    
    fields = Param(Params._dummy(), "fields", 
                         "new fields",
                         typeConverter=TypeConverters.toList)

    def __init__(self):
        super(HasFieldValues, self).__init__()
        
    def setValues(self, value):
        return self._set(values=value)

    def getValues(self):
        return self.getOrDefault(self.values)

    def setFields(self, value):
        return self._set(fields=value)

    def getFields(self):
        return self.getOrDefault(self.fields)


class FillMode(Params):

    fill_mode = Param(Params._dummy(), "fill_mode", 
                         "should disassembler fill mode first",
                         typeConverter=TypeConverters.toBoolean)

    def __init__(self):
        super(FillMode, self).__init__()
        self._setDefault(fill_mode=False)

    def setFillMode(self, value):
        return self._set(fill_mode=value)

    def getFillMode(self):
        return self.getOrDefault(self.fill_mode)