import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;


public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        List<MapTask> mapTasks = List.of(
                new MapTask("file1.txt"),
                new MapTask("file2.txt"),
                new MapTask("file3.txt")
        );

        var coordinator = new WordCounter(mapTasks, 4);
        System.out.println(coordinator.count());
    }
}

class MapTask {
    private final UUID id;
    private final String filename;

    public MapTask(String filename) {
        this.id = UUID.randomUUID();
        this.filename = filename;
    }

    public UUID getId() {
        return id;
    }

    public String getFilename() {
        return filename;
    }
}

class WordCounter {

    private final List<MapTask> mapTasks;
    private final Integer reduceWorkersCount;

    public WordCounter(List<MapTask> mapTasks, Integer reduceWorkersCount) {
        this.mapTasks = mapTasks;
        this.reduceWorkersCount = reduceWorkersCount;
    }

    public Map<String, Integer> count() throws InterruptedException, ExecutionException {

        ExecutorService mapExecutor = Executors.newFixedThreadPool(mapTasks.size());
        List<Future<Map<String, Integer>>> futureList = new ArrayList<>();

        for (MapTask mapTask : mapTasks) {
            MapCallable mapCallable = new MapCallable(mapTask);
            Future<Map<String, Integer>> future = mapExecutor.submit(mapCallable);
            futureList.add(future);
        }

        mapExecutor.shutdown();
        mapExecutor.awaitTermination(1, TimeUnit.SECONDS);

        // SHUFFLE
        Map<String, List<Integer>> shuffledMetrics = new HashMap<>();
        for (Future<Map<String, Integer>> mapFuture : futureList) {
            Map<String, Integer> mappedMetrics = mapFuture.get();
            mappedMetrics.forEach((key, value) -> {
                List<Integer> metricCountList = shuffledMetrics.getOrDefault(key, new ArrayList<>());
                metricCountList.add(value);
                shuffledMetrics.put(key, metricCountList);
            });
        }

        // REDUCE
        ExecutorService reduceExecutor = Executors.newFixedThreadPool(reduceWorkersCount);
        List<Future<Map.Entry<String, Integer>>> futureReducerList = new ArrayList<>();
        shuffledMetrics.forEach((key, value) -> {
            ReduceCallable reduceCallable = new ReduceCallable(key, value);
            Future<Map.Entry<String, Integer>> futureReducer = reduceExecutor.submit(reduceCallable);
            futureReducerList.add(futureReducer);
        });

        reduceExecutor.shutdown();
        reduceExecutor.awaitTermination(1, TimeUnit.SECONDS);

        // GET RESULTS
        Map<String, Integer> resultMap = new HashMap<>();
        for (Future<Map.Entry<String, Integer>> futureEntry : futureReducerList) {
            Map.Entry<String, Integer> entry = futureEntry.get();
            resultMap.put(entry.getKey(), entry.getValue());
        }

        return resultMap;
    }
}

class MapCallable implements Callable<Map<String, Integer>> {
    private final MapTask mapTask;
    private final Map<String, Integer> logMap;

    public MapCallable(MapTask mapTask) {
        this.mapTask = mapTask;
        this.logMap = new HashMap<>();
    }

    @Override
    public Map<String, Integer> call() throws Exception {

        String content = new String(Files.readAllBytes(Paths.get(mapTask.getFilename())));
        String[] words = content.split("\\s+");

        for (String word : words) {
            String key = word.trim();
            int valueToPut = logMap.getOrDefault(key, 0) + 1;
            logMap.put(key, valueToPut);
        }

        System.out.println("Map task " + mapTask.getId() + " done!");

        return logMap;
    }
}

class ReduceCallable implements Callable<Map.Entry<String, Integer>> {

    private final String metricName;
    private final List<Integer> metricCounts;

    public ReduceCallable(String metricName, List<Integer> metricCounts) {
        this.metricName = metricName;
        this.metricCounts = metricCounts;
    }

    @Override
    public Map.Entry<String, Integer> call() {
        int total = 0;
        for (Integer metricCount : metricCounts) {
            total += metricCount;
        }

        return Map.entry(metricName, total);
    }
}