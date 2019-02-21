module ML

open System
open System.IO
open Microsoft.ML
open Microsoft.ML.Data

type SentimentData () =
    [<DefaultValue>]
    [<LoadColumn(0)>]
    val mutable public SentimentText :string

    [<DefaultValue>]
    [<LoadColumn(1)>]
    val mutable public Label :bool
    
    // NOTE: Need to add this column to extract metrics
    [<DefaultValue>]
    [<LoadColumn(2)>]
    val mutable public Probability :float32

type SentimentPrediction () =
    [<DefaultValue>]
    val mutable public SentimentData :string

    [<DefaultValue>]
    val mutable public PredictedLabel :bool

    [<DefaultValue>]
    val mutable public Score :float32 

let modelFile = __SOURCE_DIRECTORY__ + "/model.zip"

// Load model from file
let loadModel (path:string) = // "test-model.zip"
    use fsRead = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
    let mlReloaded = MLContext()
    let modelReloaded = TransformerChain.LoadFrom(mlReloaded, fsRead)
    modelReloaded.CreatePredictionEngine<SentimentData, SentimentPrediction>(mlReloaded)
    

let predictorReloaded = loadModel modelFile

let prediction (predictorReloaded : PredictionEngine<SentimentData, SentimentPrediction>) text =
    let test = SentimentData()
    test.SentimentText <- text
    predictorReloaded.Predict(test)
