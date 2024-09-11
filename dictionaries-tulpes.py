all_formats = (
    ("1", "Paperback"),
    ("2", "Hardcover"),
    ("3", "Kindle Edition")
)

all_sort = (
    ("1", "relevanceexprank"),
    ("2", "salesrank"),
    ("3", "price"),
    ("4", "-price"),
    ("5", "reviewrank_authority"),
    ("6", "daterank")
)

# Subjects for books
all_subjects = (
    # ("All Subjects", "All Subjects"),
    # ("1", "Arts & Photography"),
    ("45", "Bargain Books"),
    ("2", "Biographies & Memoirs"),
    ("3", "Business & Investing"),
    # ("4", "Children's Books"),
    ("12290", "Christian Book & Bibles"),
    # ("4366", "Comics & Graphic Novels"),
    # ("6", "Cookbooks, Food & Wine"),
    # ("48", "Crafts, Hobbies & Home"),
    # ("5", "Computers & Technology"),
    # ("21", "Education & Reference"),
    ("301889", "Gay & Lesbian"),
    ("10", "Health, Fitness & Dieting"),
    # ("9", "History"),
    ("86", "Humor & Entertainment"),
    # ("10777", "Law"),
    ("17", "Literature & Fiction"),
    # ("13996", "Medicine"),
    ("18", "Mystery, Thriller & Suspense"),
    ("20", "Parenting & Relationships"),
    ("3377866011", "Politics & Social Sciences"),
    # ("173507", "Professional & Technical Books"),
    ("22", "Religion & Spirituality"),
    ("23", "Romance"),
    # ("75", "Science & Math"),
    ("25", "Science Fiction & Fantasy"),
    # ("26", "Sports & Outdoors"),
    ("28", "Teens"),
    # ("27", "Travel")
)

# Months
months = (
    ("1", "Jan"),
    ("2", "Feb"),
    ("3", "Mar"),
    ("4", "Apr"),
    ("5", "May"),
    ("6", "Jun"),
    ("7", "Jul"),
    ("8", "Aug"),
    ("9", "Sep"),
    ("10", "Oct"),
    ("11", "Nov"),
    ("12", "Dec")
)

# Sorting options
sort_bys = (
    ("relevanceexprank", "Featured"),
    ("salesrank", "Bestselling"),
    ("price", "Price: Low to High"),
    ("-price", "Price: High to Low"),
    ("reviewrank_authority", "Avg. Customer Review"),
    ("daterank", "Publication Date")
)

# Mapping from index to subject
dicti = (
    "1", "45", "2", "3", "12290", "301889", "10", "86", "17", "18", "20", "3377866011", "22", "23", "25", "28"
)