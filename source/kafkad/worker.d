module kafkad.worker;

enum WorkerType { Producer, Consumer };

/// worker may be either a producer or a consumer
interface IWorker {
    @property WorkerType workerType();
    @property string topic();
    @property int partition();
    /// throws exception in the worker task
    void throwException(Exception ex);
}