# Amazon-books
Telegram bot in Python that:

1. The `Amazon_a.py` -> starts a browser and retrieves a proxy from the database.
2. Navigates to the [Amazon Books advanced search page](https://www.amazon.com/advanced-search/books), selects search parameters such as category, language, year, sort, etc., and performs the search (clicks the search button).
3. Iterates through the search results from the 1st to the 65th page of Amazon Books, parsing author "about" page links and saving them into the database.
4. The `Amazon_b.py` -> takes links to the author's "about" page, scrapes their email, Facebook, and website, and stores this information in the database.

[Short Video on YouTube](https://youtube.com/shorts/__l4pE849Tc?si=H3bDM2uwxuvl25Jv)

[Article on kolodych.com](https://kolodych.com/articles/first-telegram-bot.html#Amazon-bot)
