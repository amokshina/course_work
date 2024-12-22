from pars_azs_new_api import AZSinfo_parcer
from pars_reviews_new_api import AZSreviews_parcer

def main():
    parser_azs = AZSinfo_parcer('config.toml', outfile_path='data/')                             
    azs_outfile = parser_azs.generate_outfile_name()
    AZSinfo_parcer.load_to_file(parser_azs, azs_outfile)
    parser_reviews = AZSreviews_parcer('config.toml', outfile_path='data/')                             
    reviews_outfile = parser_reviews.generate_outfile_name()
    AZSreviews_parcer.load_to_file(parser_reviews, reviews_outfile)

if __name__ == '__main__':
    main()