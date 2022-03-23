public class Server {
    String type;
    int id;
    String state;
    int curStartTime;//current start time
    int core;
    int memory;
    int disk;
    int wJobs; //waiting jobs
    int rJobs; //running jobs

    public Server(String type, int id,
                   String state, int sTime,
                   int core, int mem, int disk,
                   int wJobs, int rJobs)
    {
        this.type = type;
        this.id = id;
        this.state = state;
        this.curStartTime = sTime;
        this.core = core;
        this.memory = mem;
        this.disk = disk;
        this.wJobs = wJobs;
        this.rJobs = rJobs;
    }
    public int availability(){
        if(this.state == "unavailable"){
            return -1;
        }
        else if(this.state == "idle"){
            return 0;
        }
        return this.curStartTime;
    } 
}
