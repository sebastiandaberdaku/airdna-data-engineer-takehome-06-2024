#!make

build-docker:
	@docker build -t data_eng:1.0 .

test: build-docker
	@docker run -it --rm --volume $(PWD):/usr/src/app --workdir /usr/src/app data_eng:1.0 sh -c "pytest tests/"

run: build-docker
	@docker run -it --rm --volume $(PWD):/usr/src/app --workdir /usr/src/app data_eng:1.0 sh -c "python src/runner.py"
