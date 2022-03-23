public class Job {
    int id;
    String type;
    int submitTime;
    int estRunTime;
    int cores;
    int memory;
    int disk;
 
    public Job(int id, String type, int sTime,
                int estTime, int cores, int mem, int disk) {
                    this.id = id;
                    this.type = type;
                    this.submitTime = sTime;
                    this.estRunTime = estTime;
                    this.cores = cores;
                    this.memory = mem;
                    this.disk = disk;
                }
 
}
