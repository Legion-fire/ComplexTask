import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ComplexTask {
    private final int id;
    private final List<Double> data;

    public ComplexTask(int id, List<Double> data) {
        this.id = id;
        this.data = data;
    }

    public int getId() {
        return id;
    }

    // Выполняет часть сложной задачи и возвращает частичный результат
    public double execute() {
        double acc = 0.0;
        for (double x : data) {
            // Имитация вычислительной нагрузки
            acc += Math.sqrt(x) * Math.log1p(x);
        }
        // Имитация задержки
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(100, 300));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return acc;
    }
}

/**
 * Исполнитель сложных задач с синхронизацией через CyclicBarrier.
 */
class ComplexTaskExecutor {
    public ComplexTaskExecutor() {
    }

    /**
     * Создаёт пул потоков, запускает несколько задач и объединяет их результаты,
     * дожидаясь на барьере завершения всех задач.
     */
    public void executeTasks(int numberOfTasks) {
        if (numberOfTasks <= 0) {
            throw new IllegalArgumentException("numberOfTasks must be > 0");
        }

        // Контейнер для частичных результатов: taskId -> partial
        ConcurrentMap<Integer, Double> partialResults = new ConcurrentHashMap<>();

        // Итоговый агрегированный результат (заполняется в barrierAction)
        AtomicReference<Double> combinedResult = new AtomicReference<>(0.0);

        // Действие барьера: выполняется одним из рабочих потоков после await() у всех
        CyclicBarrier barrier = new CyclicBarrier(numberOfTasks, () -> {
            double total = partialResults.values().stream().mapToDouble(Double::doubleValue).sum();
            combinedResult.set(total);
            System.out.println("Barrier action: combined result = " + total
                    + " (from " + partialResults.size() + " partials)");
        });

        ExecutorService pool = Executors.newFixedThreadPool(numberOfTasks);
        List<Future<?>> futures = new ArrayList<>(numberOfTasks);

        for (int i = 0; i < numberOfTasks; i++) {
            final int taskId = i;
            final List<Double> data = generateDataForTask(taskId);
            final ComplexTask task = new ComplexTask(taskId, data);

            futures.add(pool.submit(() -> {
                try {
                    double partial = task.execute();
                    partialResults.put(task.getId(), partial);
                    System.out.println("Task " + task.getId() + " produced partial = " + partial);
                } catch (Exception e) {
                    // В случае ошибки фиксируем частичный результат как 0,
                    // чтобы барьерное действие могло корректно отработать
                    partialResults.putIfAbsent(taskId, 0.0);
                    System.err.println("Task " + taskId + " failed: " + e);
                } finally {
                    try {
                        barrier.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (BrokenBarrierException bbe) {
                        System.err.println("Barrier broken for task " + taskId + ": " + bbe);
                    }
                }
            }));
        }

        // Дожидаемся завершения всех задач
        pool.shutdown();
        try {
            for (Future<?> f : futures) {
                f.get(); // каждая задача вернётся только после прохождения барьера
            }

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException ee) {
            System.err.println("Execution error: " + ee.getCause());
        }

        System.out.println("Final combined result in main: " + combinedResult.get());
    }

    private List<Double> generateDataForTask(int taskId) {
        ArrayList<Double> list = new ArrayList<>(1000);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < 1000; i++) {
            // Набор данных для задачи
            list.add(rnd.nextDouble(0.1, 10.0) + taskId);
        }
        return list;
    }
}

