# type: ignore

# Add the libs to the path of execution for local execution
from nltk.sentiment.vader import SentimentIntensityAnalyzer  

def get_sentiment(text, lang="en"):
    """
    Uses NLTK vader algorithm
     The 'compound' score is computed by summing the valence scores of each word in the lexicon, adjusted
     according to the rules, and then normalized to be between -1 (most extreme negative) and +1 (most extreme positive).
     This is the most useful metric if you want a single unidimensional measure of sentiment for a given sentence.
     Calling it a 'normalized, weighted composite score' is accurate
     
     The 'pos', 'neu', and 'neg' scores are ratios for proportions of text that fall in each category (so these
     should all add up to be 1... or close to it with float operation).  These are the most useful metrics if
     you want multidimensional measures of sentiment for a given sentence
    """
    
    analyser = SentimentIntensityAnalyzer()
    o = analyser.polarity_scores(text)
    com = o['compound']
    if com > 0:
        sen = "POSITIVE"
    elif com < 0:
        sen = "NEGATIVE"
    else:
        sen = "NEUTRAL"
    return sen, o['compound'], o['pos'], o['neg']

# Test the functions
if __name__ == '__main__':
    x = "Apple is sinking hard"
    print(get_sentiment(x))