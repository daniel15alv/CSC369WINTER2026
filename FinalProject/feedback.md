# 1: Clarifying the Definition of “New User”
Problem: Based on feedback, this analysis is worded as “new user analysis”, when in reality, it tracks 
analysis on first observed review for a given user in 2018.
Solution: The final report was revised to redefine a “new user” as” a reviewer’s first observed rating 
in 2018”. The rest of the report then was updated to reflect this definition.

# 2: Interpretation of “Sentiment”
Problem: Amazon review data measures product satisfaction, not sentiment towards Amazon as a company.
Solution: Similar to the first issue, wording was changed to reflect what was truly analyzed. The 
research question and introduction to revised to accordingly focus on: How reviewer’s product ratings 
change during their first 90 days of reviewing activity, removing the implication that the analysis 
measures attitude towards Amazon itself.

# 3: Histogram Spike Confusion
Problem: Pierce expressed confusion as to why the histogram of sentiment change containe spikes at 
whole numbers.
Solution: Under the plot, I briefly explained that Amazon ratings are discrete from 1 to 5, resulting 
in differences occurring at integer or half-integer values (like -0.5, -0.5) when averages were 
compared over time. 

# 4: Regression to the Mean in the Conclusion
Problem: The subgroup results may simply reflect the idea of regression to the mean rather than 
meaningful behavioral changes.
Solutions: I rewrote the conclusion to avoid casual claims, included an entire paragraph explaining 
the regression to the mean, and cautiously interpreted my subgroup findings to reflect this idea.

# 5: Include Additional Statistical Tests for Subgoups
Based on feedback, it was suggested that I perform additional statistical tests to determine whether 
subgroup patterns were statistically significant.
Solution: I performed a second set of one-sample t-tests for each initial rating group, as well as 95% 
confidence intervals for the mean chane within each group; I added these as well as one visualization 
that was simply an update to an earlier plot, now including confidence interval error bars.

