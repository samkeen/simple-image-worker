import os
from PIL import Image

here = os.path.abspath(os.path.dirname(__file__))
imagePath = os.path.join(here, 'test.jpg')

size = (128, 128)

pathParts = os.path.splitext(imagePath)
outfile = pathParts[0] + ".thumbnail" + pathParts[1]
if imagePath != outfile:
    try:
        im = Image.open(imagePath)
        im.thumbnail(size)
        im.save(outfile, "JPEG")
    except IOError:
        print("cannot create thumbnail for", imagePath)
