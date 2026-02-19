Data Parsing
The original dataset consisted of a single ~127GB line-delimited JSON file. Loading this file into memory directly was not feasible. Instead, the data was processed using a streaming ingestion approach, 
reading the file line-by-line.
To prevent memory overflow and switch from raw JSON,each line was…
Validated for required fields (i.e., reviewer ID, unix review time, etc)
Filtered according to year and sampling rules
Written to smaller parquet files, with 200k rows per file

Quality Issues
User activity is extremely sparse, with about 125,233 total users, but 72.4% of them leaving exactly one review only. The median number of reviews per user is 1, and the 90th percentile of reviews per user is 3; 
most users do not have sufficient history to measure their sentiment change over time, especially if this only throughout the course of one year. To resolve this, analysis is restricted to users with activity in both 
early and late windows (within their first 90 days) 

Additionally, there is a positive rating skew present. 66% of reviews are 5-stars, and 11% are 4-stars. This may imply that most users who write a review are the ones who are happy with a product; many people who 
feel indifferent don’t even bother to write a review. Because the baseline sentiment is highly positive, detecting meaningful shifts may be statistically difficult. To resolve this, rather than analyzing raw rating levels, 
changes were measured within-user (late minus early average rating).

Although the full dataset spans from 2018 through 2021, the cap on rows limited this dataset to only reviews from 2018. As a direct consequence of this, cross-year behavioral shifts can not be evaluated; 
Findings are limited to within-year dynamics, which may not feel “meaningful”. To resolve this, the analysis will be re-framed to “within-year” analysis rather than simply “overtime”

