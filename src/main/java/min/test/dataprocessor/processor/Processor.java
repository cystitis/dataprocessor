package min.test.dataprocessor.processor;

public interface Processor {
    /**
     * Processor 에서 사용할 thread 를 initialize 한다.
     * @param size thread 개수
     */
    void initWorkers(int size);

    /**
     * Processor 의 모든 thread 를 start 한다.
     */
    void start();

    /**
     * Processor 의 동작중인 모든 thread 를 shutdown 한다.
     */
    void shutdown();
}
