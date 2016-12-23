from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
from itertools import combinations

class Movie_Similarities(MRJob):
    
    def configure_options(self):
        super(Movie_Similarities, self).configure_options()
        self.add_file_option('--dataset', help='F:\Dropbox\UNCC\Fall 2016\Cloud Data ITIS 6320\hadoop assignment\scripts\movies.dat')
	self.add_passthrough_option('--m', help='F:\Dropbox\UNCC\Fall 2016\Cloud Data ITIS 6320\hadoop assignment\scripts\movies.dat')
		
# Load dataset		
    def movie_titles(self):
        
        self.movieNames = {}
        with open("F:\Dropbox\UNCC\Fall 2016\Cloud Data ITIS 6320\hadoop assignment\scripts\movies.dat") as f:
            for line in f:
                fields = line.split('::')
                self.movieNames[int(fields[0])] = fields[1]
        
    def steps(self):
        return [
            MRStep(mapper=self.map_input,
                    reducer=self.reducer_user_ratings),
            MRStep(mapper=self.map_item_pairs,
                    reducer=self.reducer_cos_similarity),
            MRStep(mapper=self.map_sort,
                    mapper_init=self.movie_titles,
                    reducer=self.reducer_similar_movies)]

#Map a user with the movie and its rating given by him 					
    def map_input(self, key, line):       
        (userID, movieID, rating, timestamp) = line.split('::')
        yield userID, (movieID,float(rating))

#Grouping list of movies & ratings based on user information        
    def reducer_user_ratings(self, userId, movieRatingsPair):		
        agg_ratings = []
        for movieId,rating in movieRatingsPair:
            agg_ratings.append((movieId,rating))
            
        yield userId, agg_ratings
 
#List all the movies viewed by the users 
    def map_item_pairs(self, userId, movie_ratings):		
        for movie_rating1, movie_rating2 in combinations(movie_ratings,2):
            movieID1 = movie_rating1[0]
            rating1 = movie_rating1[1]
            movieID2 = movie_rating2[0]
            rating2 = movie_rating2[1]
            
            yield(movieID1,movieID2),(rating1,rating2)
            yield(movieID2,movieID1),(rating2,rating1)
    
 #Calculate cosine similarity   
    def cosine_similarity(self,mv_rate_pair):       
        numPairs = 0
        sumxx = sumyy = sumxy = 0
        
        for ratingX,ratingY in mv_rate_pair:
            sumxx += ratingX * ratingX
            sumyy += ratingY * ratingY
            sumxy += ratingX * ratingY
            numPairs += -1
        
        num = sumxy
        denom = sqrt(sumxx) * sqrt(sumyy)
        
        cos_sim_value = 0
        if (denom):
            cos_sim_value = (num/(float(denom)))
        
        return (cos_sim_value, numPairs)
    
	def min_rate_similarity(self,x,y):
		intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
		union_cardinality = len(set.union(*[set(x), set(y)]))
		cardinality = intersection_cardinality/float(union_cardinality)
		return cardinality

# Compute the similarity score for each movie pair viewed by multiple users		
    def reducer_cos_similarity(self, mv_pair, mv_rate_pair):       
        cos_sim_value, numPairs = self.cosine_similarity(mv_rate_pair)
# Setting minimum score and minimum number of ratings for similar movies        
        if (numPairs>25 and cos_sim_value > 0.97):
            yield mv_pair, (cos_sim_value,numPairs)

#Sort and display movies			
    def map_sort(self, mv_pair, similarityMatrixPair):
        cos_sim_value, total_rating = similarityMatrixPair
        movieId1,movieId2 = mv_pair
        
        yield (self.movieNames[int(movieId1)], cos_sim_value), \
        (self.movieNames[int(movieId2)], total_rating)

# Gives the movies similar to a particular movie, its total ranking and score		
    def reducer_similar_movies(self, movieScore, similarN):
        movie1, cos_sim_value = movieScore
        for movie2, total_rating in similarN:
            yield movie1, (movie2,cos_sim_value,total_rating)


if __name__ == "__main__":
    Movie_Similarities.run()