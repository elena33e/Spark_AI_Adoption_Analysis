# AI Adoption & Workforce Impact Analysis - Apache Spark

This project utilizes Apache Spark to quantify the real impact of Artificial Intelligence adoption on productivity and the workforce[cite: 1]. The distributed analysis system correlates company-specific AI data with macroeconomic indicators and sector benchmarks[cite: 1].

## Dataset

The analysis is based on the *Global AI Adoption & Workforce Impact Dataset*, which contains over 150,000 records divided into three CSV files[cite: 1]:

*   **`ai_company_adoption.csv`**: The main file detailing individual company performance[cite: 1]. It includes columns such as `company_id`, `country`, `industry`, `ai_adoption_rate`, `jobs_created`, `jobs_displaced`, and `annual_revenue_usd_millions`[cite: 1].
*   **`country_ai_index.csv`**: Contains geopolitical and infrastructure metrics at the country level[cite: 1]. Includes columns like `country`, `region`, `digital_maturity_index`, and `country_ai_policy`[cite: 1].
*   **`ai_industry_summary.csv`**: Provides benchmarks for various economic sectors[cite: 1]. The main columns are `industry`, `avg_ai_adoption_rate`, and `avg_ai_failure_rate`[cite: 1].

## Architecture and Spark Optimizations

The application is structured on the principle of separation of concerns, being divided into three main classes[cite: 1]:

*   **`SparkConfig`**: Initializes the Spark session and configures the application to run in `local[*]` mode to utilize all available CPU cores on the local machine[cite: 1].
*   **`DataLoader`**: Manages the loading and structural validation of the data[cite: 1]. 
    *   Applies an explicitly defined schema for the main dataset, optimizing the processing of the 150,000 records[cite: 1]. 
    *   Uses a generic method for dynamic loading of the reference files[cite: 1]. 
    *   Implements RDD-level preprocessing to filter out null data (e.g., budgets with a value of 0), a process monitored through an accumulator that tracks data quality[cite: 1].
*   **`AnalysisService`**: Executes data processing and analysis queries using a combination of Spark DSL and SQL[cite: 1]. 
    *   Performs a triple *Broadcast Join* operation to merge the main dataset with the secondary files[cite: 1]. 
    *   Applies data persistence in memory (`MEMORY_ONLY`) to ensure optimal query execution speed[cite: 1]. 
    *   Distributes the results either through direct console output or by saving analytical reports as CSV files in the `/results` directory[cite: 1].

## Queries and Analytical Reports

The application executes a set of 10 exploratory and analytical queries on the memory-persisted data[cite: 1]:

**Data Exploration:**
*   Identifying companies in Europe with an AI adoption rate higher than 70%[cite: 1].
*   Generating a top 10 list of companies sorted in descending order by their innovation score (`innovation_score`)[cite: 1].
*   Filtering startups that allocate more than 15% of their total budget to AI initiatives[cite: 1].
*   Listing the primary AI tools used specifically in the Technology industry[cite: 1].
*   Ranking the top 5 countries based on the density of AI researchers per one million inhabitants[cite: 1].

**Business Analysis and Economic Impact:**
*   **Productivity vs. National Policies:** Calculates average corporate productivity growth based on government AI policies (e.g., "Incentive-based", "Lenient", "Strict")[cite: 1].
*   **Workforce Dynamics (Net Job Change):** Evaluates the net balance of jobs per industry by subtracting displaced jobs (`jobs_displaced`) from newly created ones (`jobs_created`)[cite: 1].
*   **Regional Adoption Stage:** Counts and groups companies according to their geographical region and AI implementation stage (e.g., "Pilot", "Integrated", "Full")[cite: 1].
*   **Revenue Growth and Digital Maturity:** Analyzes average revenue growth exclusively for countries with a digital maturity index greater than 85[cite: 1].
*   **Identifying Failure Risks (Outliers):** Compares each company's multi-year failure rate with its industry average to identify organizations performing below the sector standard[cite: 1].
