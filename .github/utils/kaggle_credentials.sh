# make .kaggle dir and write kaggle.json
mkdir ~/.kaggle 

echo "{\"username\":\"tacosdedatos\",\"key\":\"${{ secrets.KAGGLE_API_KEY }}\"}" > ~/.kaggle/kaggle.json