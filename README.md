# custom-spark-pipeline

# app/tykuo_spark_model
## ImputeCategoricalWithModeModel
* Impute categorical features with mode

## StringDisassembler (One Hot)
* Disassemble categorical feature into multiple binary columns 

## VectorDisassembler
* Disassemble vector feature into multiple numeric columns 

# Examples
## mode-imputer
* Impute categorical features with mode
* Combine with spark 2.3 imputer into savable pipeline

## custom-onehot
* StringDisassembler vs OneHotEncoderEstimator

## vec-disassembler
* Try VectorDisassembler

## import-tykuo-model
* Put all custom feature estimators together

