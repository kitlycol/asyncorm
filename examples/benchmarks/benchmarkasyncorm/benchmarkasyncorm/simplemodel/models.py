from asyncorm import models

BOOK_CHOICES = (
    ('hard cover', 'hard cover book'),
    ('paperback', 'paperback book')
)


class Book(models.Model):
    name = models.CharField(max_length=50)
    content = models.CharField(max_length=255, choices=BOOK_CHOICES)
    date_created = models.DateField(auto_now=True)
    price = models.DecimalField(default=25, decimal_places=2, max_digits=12)
    quantity = models.IntegerField(default=1)

    class Meta():
        ordering = ['-id', ]
