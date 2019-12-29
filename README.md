# Final Project - Analysis of the Yelp Dataset Using Spark

## Background

The Yelp dataset was originally released in order for students to do research and analysis in to how food trends begin and how they impact locations.  The dataset includes data about businesses, reviews, users, checkins, tips, and photos.

I chose this dataset because I have always been very interested in social media platforms like Yelp. I find the interactions that people on social media have to be very interesting and saw this as a good opportunity to further explore these interactions.  By looking at the Yelp reviews I hoped to discover a correlation between the content of a review and how it is actually rated.  

## Basic Questions

**How many reviews are in the Yelp dataset?**

There are 6,685,900 reviews in the Yelp dataset.

**What is the average rating of a review on Yelp?**

The average rating of a review on Yelp is 3.72 stars.

**What is the average length of a text review on Yelp?**

The average length of a text review on Yelp is 602.78 characters.

**What types of restaurants have the most Yelp reviews?**

Mexican restaurants have the most Yelp reviews with a total of 105,643 reviews.

**Which day of the week are the most reviews posted?**

The day when the most Yelp reviews are posted is Sunday with 1023438 reviews.

**Which month of the year are more reviews posted?**

The month when the most Yelp reviews are posted is July with 649136 reviews.

**What city has the most Yelp Reviews?**

The city with the most Yelp reviews is Las Vegas with 2,030,798 reviews.

![Question 4](plots/LasVegasBusinessReviews.png)

The Business Reviews In Las Vegas plot shows the businesses in the city of Las Vegas plotted using their longitude and latitude.  The rating is colored green, yellow and red; with a max rating of 5.0, a middle rating of 2.5, and a lowest rating of 0.0 respectively.

**What is the average rating for a Yelp review that is found useful?**

The average rating for a Yelp review that is found useful is 3.53 stars whith an average amount of useful votes of 2.91.

![Question 8](plots/ReviewUsefulness.png)

The Review Usefulness and Text Length plot shows each review with more than 3 useful votes (the average number of useful votes) plotted by the number of useful votes and the length of the review by characters. The rating is colored green, yellow and red; with a max rating of 5.0, a middle rating of 2.5, and a lowest rating of 0.0 respectively.

**What is the average rating for a Yelp review that is found funny?**

The average rating for a Yelp review that is found funny is 3.53 stars whith an average amount of funny votes of 0.95.

![Question 9](plots/ReviewFunniness.png)

The Review Funniness and Text Length plot shows each review with more than 1 funny votes (the average number of funny votes) plotted by the number of funny votes and the length of the review by characters. The rating is colored green, yellow and red; with a max rating of 5.0, a middle rating of 2.5, and a lowest rating of 0.0 respectively.

**What is the average rating for a Yelp review that is found cool?**

The average rating for a Yelp review that is found cool is 3.53 stars whith an average amount of cool votes of 1.16.

Surprisingly there was no difference in the average rating of a Yelp review for useful, funny, or cool.
![Question 10](plots/ReviewCoolness.png)

The Review Coolness and Text Length plot shows each review with more than 1 cool votes (the average number of cool votes) plotted by the number of cool votes and the length of the review by characters. The rating is colored green, yellow and red; with a max rating of 5.0, a middle rating of 2.5, and a lowest rating of 0.0 respectively.

## SparkML

**Is there a relationship between a Yelp review's text length, usefulness, funniness, and coolness, and whether the review has a higher or lower rating?**

Using a linear regression using the different combinations of fields where the votes was greater than 150 the average error was the following. I found that by changing the number of votes for each of the attributes the average error decreased, but only to a certain point.

| Fields | Average Error |
| ------ | ------------- |
| length(text) | 1.2058170782570148 |
| useful, length(text) | 1.2238439310941533 |
| funny, length(text) | 1.1734020572942112 |
| cool, length(text) | 0.8842517840713103 |
| useful, funny, cool, length(text) | 0.8082418179918148 |

It is clear that the length of a review's text does not correlate with with the rating a review is given.  Therefore here are the average error of the usefulness, funniness, and coolness fields in groups of 2 and together.

| Fields | Average Error |
| ------ | ------------- |
| useful, cool | 1.2157646129308195 |
| useful, funny | 1.2430437738770561 |
| funny, cool | 1.236279590293341 |
| useful, funny, cool| 0.8218244288898473 |

It is also clear that not looking at the length of a review's text and just the usefulness, funniness, and coolness fields themselves does not improve the average error and actually makes it worse.  Therefore, text length does seem to play a role in predicting the rating of a review; just not a large role.

![Question 1](plots/Regression.png)

The plot confirms that using the length of a review's text, number of useful votes, number of funny votes, and number of cool votes alone does not predict the rating of a review very well, but there are a few outliers that seem to show that in certain situations it is a good predictor.  Those being when the rating is very low and the text length is high and when the text length is around 1500 and the rating is around a 4.

![Question 2](plots/Kmeans1.png)

Using a Kmeans Classifier with K = 5, I was able to get more accurate predictions for higher ratings using the review's text length, number of useful votes, number of funny votes, and number of cool votes.

The reason why my method for predicting the rating of a review based on a review's text length, usefulness, funniness, and coolness is because altough these metrics tell a lot they do not tell what is actually in the review.  It is hard to tell whether the length of a review is because the user has a lot of positive things to say or negative.  The usefulness, funniness, and coolness votes dont mean much as far as the language that is in the review either.

Apache OpenNLP is a scala Natural Language Processing library that I could use to get better predictions about the rating of a review from the review's content directly.

Documentation for the Yelp Dataset can be found at https://www.yelp.com/dataset/documentation/main.

Documentation for the City Dataset can be found at https://public.opendatasoft.com/explore/dataset/1000-largest-us-cities-by-population-with-geographic-coordinates/table/?sort=-rank.
