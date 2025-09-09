# Big Data Analytics with PySpark

## Introduction  

From tweets and movies to taxis and system logs — this project explores how to uncover insights from millions of records using the Spark ecosystem.  

Instead of just running calculations, the goal was to see how PySpark can handle different types of big data challenges:  
- Social media: 14M tweets — where, when, and how people tweet  
- Entertainment: 33M movie ratings — what genres dominate, and who the power users are  
- Urban transport: Taxi trips as a graph — running PageRank to find the most connected areas  
- Systems monitoring: Real-time HDFS logs — detecting host activity with Structured Streaming  

This project shows how Spark enables seamless transitions between:  
- Batch analytics (Twitter, MovieLens)  
- Graph analytics (Chicago Taxi trips)  
- Streaming analytics (HDFS logs)  

All while working at massive scale with clean, reproducible pipelines.  

The repository includes code, notebooks, outputs, and a detailed report for each task.  

---

## Repository Structure
- `task1/` → Twitter Data Analysis (14M tweets)
- `task2/` → MovieLens Ratings (33M ratings)
- `task3/` → Chicago Taxi Trips (GraphFrames)
- `task4/` → HDFS Logs Streaming (Structured Streaming)
- `report.pdf` → Detailed project report with methodology, results, and insights

---

## Task 1: Twitter Data Analysis
**Dataset:** 14,262,517 tweets (U.S., Jan 12–18, 2013)  
**Technologies:** PySpark (RDD & DataFrame APIs), Matplotlib  

### Objectives
- Load & preprocess Twitter data (timestamps, geolocation, weekdays).  
- Analyze geospatial distribution of tweets.  
- Explore temporal patterns (time of day, weekday activity).  
- Detect above-average tweet days.  
- Identify top active locations.  

### Implementation
- Converted timestamps to UTC, extracted weekdays & hours.  
- Classified tweets into Morning, Afternoon, Evening, Night.  
- Grouped by geolocation, aggregated counts.  
- Plotted weekly & time-of-day patterns.  

### Insights
- Evenings showed the highest tweet activity.  
- Sunday was the most active day; Friday the least.  
- Identified above-average tweet days and top 10 hotspots.  

### Code Reference
- `task1/task1.py` → main PySpark job  
- `task1/task1_Q3.ipynb` → Geospatial scatter plot  
- `task1/task1_Q4.ipynb` → Time-of-day analysis  
- `task1/task1_Q5.ipynb` → Weekday aggregation  
---

## Task 2: MovieLens Ratings Data Analysis
**Dataset:** MovieLens (33M ratings across 20+ years)  
**Technologies:** PySpark (SQL & DataFrame API), Matplotlib  

### Objectives
- Categorize ratings into Low / Medium / High.  
- Analyze time-based patterns (early vs. late year, yearly growth).  
- Study ratings by genre.  
- Identify top-rated movies (≥100 ratings).  
- Classify users by activity level.  

### Implementation
- Preprocessing: Converted timestamps → years/months, joined `ratings.csv` + `movies.csv`.  
- Analysis:  
  - Ratings: Medium (3.0–4.5) made up ~68%.  
  - Seasonal distribution: Ratings evenly split early vs. late year.  
  - Genres: Drama, Comedy, Action were most popular.  
  - Top Movies: Extracted best-rated films (≥100 reviews).  
  - Yearly trend: Peaks near 2000 & 2015 (~2M ratings/year).  
  - Users: 128k Frequent Raters vs. 202k Infrequent Raters.  

### Code Reference
- `task2/task2.py` → full PySpark job  
- `task2/task2_Q3.ipynb` → Rating categories  
- `task2/task2_Q4.ipynb` → Seasonal trends  
- `task2/task2_Q5.ipynb` → Ratings by genre  
- `task2/task2_Q6.ipynb` → Yearly distribution  
- `task2/task2_Q8.ipynb` → User activity  
---

## Task 3: Chicago Taxi Trips Data Analysis
**Dataset:** Chicago Taxi Trips (Jan 2024 onward)  
**Technologies:** PySpark, GraphFrames, Matplotlib  

### Objectives
- Model trips as a graph of community areas.  
- Perform shortest path analysis.  
- Compute PageRank (unweighted & weighted by fares).  
- Analyze fare distribution by distance, duration, and time of day.  

### Implementation
- Graph Construction:  
  - Vertices = Community Areas (with census + coordinates).  
  - Edges = Trips between areas (with miles, duration, fare).  
- Graph Analysis:  
  - Shortest Paths: BFS from all nodes to community area 49.  
  - PageRank: Compared unweighted (connections) vs. weighted (fares).  
- Fare Analysis:  
  - Peak around 4–6 AM.  
  - Longer trips cost more (distance & duration).  

### Code Reference
- `task3/task3.py` → full PySpark + GraphFrames pipeline  
- `task3/task3_Q6.ipynb` → PageRank (unweighted/weighted)  
- `task3/task3_Q7.ipynb` → Fare analysis  
---

## Task 4: HDFS Logs Streaming Analysis
**Dataset:** HDFS logs (streaming input from socket)  
**Technologies:** PySpark Structured Streaming  

### Objectives
- Stream log data from socket input.  
- Parse date, time, component, host, block size.  
- Apply watermarking for late events.  
- Monitor DataNode activity.  
- Aggregate block sizes per host.  
- Count INFO-level log entries by host.  

### Implementation
- Log Preprocessing: Extracted date/time → unified timestamp column.  
- Streaming Queries:  
  - Q1/Q2: Basic timestamp extraction + watermarking.  
  - Q4: DataNode log counts (sliding windows).  
  - Q5: Aggregated block size per host → ranked hosts by usage.  
  - Q6: Counted INFO-level block events → top active hosts.  

### Code Reference
- `task4/task4.py` → Streaming pipeline with Q1–Q6 queries  
---

## Key Takeaways
- Batch Analytics: Efficient large-scale data analysis with PySpark.  
- Graph Analytics: Network science methods applied to transport data.  
- Streaming Analytics: Real-time monitoring of system logs.  

This project highlights how PySpark, GraphFrames, and Structured Streaming can handle diverse big data challenges in social media, entertainment, urban transport, and distributed systems.  
