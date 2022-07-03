# Big Data Benckmark: Spark vs Hadoop

Comparison between various implementations of a trivial ranking and a graph algorithm in Spark and hadoop in terms of performance, development time and cluster setups.

# Data Set

The [Reporting Carrier On-Time Performance Dataset](https://developer.ibm.com/exchanges/data/all/airline/) contains information on approximately 200 million domestic US flights reported to the United States Bureau of Transportation Statistics. The dataset contains basic information about each flight (such as date, time, departure airport, arrival airport) and, if applicable, the amount of time the flight was delayed and information about the reason for the delay.

**More Details:**

| Property          | Description                            |
|:-----------------:|:--------------------------------------:|
| Size              | 81 GB                                  |
| Number of Records | 194,385,636 flights                    |
| Date              | 1987 through 2020                      |
| Dataset Origin    | US Bureau of Transportation Statistics |

# Algorithms

- **Graph**

  **Breadth-first search (BFS)** is an algorithm that is used to graph data or searching tree or traversing structures. The full form of BFS is the Breadth-first search. The algorithm efficiently visits and marks all the key nodes in a graph in an accurate breadthwise fashion. This algorithm selects a single node (initial or source point) in a graph and then visits all the nodes adjacent to the selected node. Remember, BFS accesses these nodes one by one. Once the algorithm visits and marks the starting node, then it moves towards the nearest unvisited nodes and analyses them. Once visited, all nodes are marked. These iterations continue until all the nodes of the graph have been successfully visited and marked.

- Trivial

  Rank each day of week in every month of every year in terms of average arrival delay, grouped by year, month, dayOfWeek.


# Result

Final result published in this detailed [document](https://drive.google.com/file/d/13WiaNPG5Htkmo6J97VEm0JMcrpMEvVWl/view?usp=sharing).
