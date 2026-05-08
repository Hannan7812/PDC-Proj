# Distributed Document Processing (PDC Project)

## Run
This project requires Java and maven installed on the computer. All the instructions to run the project use maven thus having maven is a requirement.
1. Build:
   - `mvn -q -DskipTests compile`
2. Start master:
   - `mvn -q exec:java -Dexec.mainClass=pdc.AppMaster`
3. Start as many workers as required workers (separate terminals):
   - `mvn -q exec:java -Dexec.mainClass=pdc.AppWorker`

Worker Count can be changed in `config/application.properties` by setting `worker.count`. Other parameters can also be changed in the same file.

In order to faciliate the testing, a separate java files have been created to run the sequential baseline and the parallel version. To run the sequential version, use the following command:
- `mvn -q exec:java -Dexec.mainClass=pdc.AppSequential`

A single unified script can be used to orchestrate and run the entire parallel version of the project instead of going to separate terminals and launching workers manually. To run the script, use the following command:
- `mvn -q exec:java -Dexec.mainClass=pdc.MultiTerminalLauncher`
After running the above command, the script will ask for the number of workers to launch. After entering the number of workers, the script will launch the master and the specified number of workers in separate terminals.

All the scripts output the results in the terminal and also write the results to metric/run_metrics.csv file. The metrics file contains the following columns:
- run_type,compute_mode,success,duration_ms,workers,threads_per_worker,total_tasks,completed_tasks,seq_part

An overall timeline of all the runs can be found in `metrics/timeline.csv` file.


## Data

Put `.txt` files in `/data` relative to project root.

