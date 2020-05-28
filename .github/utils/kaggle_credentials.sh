# make .kaggle dir and write kaggle.json
mkdir ~/.kaggle 

echo "{\"username\":\"tacosdedatos\",\"key\":\"${KAGGLE_API_KEY}\"}" > ~/.kaggle/kaggle.json

chmod 600 ~/.kaggle/kaggle.json