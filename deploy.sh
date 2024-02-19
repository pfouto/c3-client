mvn clean package

echo "Copying to cluster"
rsync -azvhuP -r --delete --exclude 'venv' --exclude=".*" deploy/* cluster:/home/pfouto/chain3/client
echo "Done"