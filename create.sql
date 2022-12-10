CREATE TABLE Threads(
	thread_id INT NOT NULL,
	thread_count INT NOT NULL,
	PRIMARY KEY(thread_id)
);

CREATE TABLE Lithography(
	lithography_id INT NOT NULL,
	lithography_size REAL NOT NULL,
	PRIMARY KEY(lithography_id)
);

CREATE TABLE LaunchYear(
	launchy_id INT NOT NULL,
	launchy_year INT NOT NULL,
	PRIMARY KEY(launchy_id)
);

CREATE TABLE CPU(
	cpu_id SERIAL,
	cpu_name char(50) NOT NULL,
	thread_id INT NOT NULL,
	lithography_id INT NOT NULL,
	launchy_id INT NOT NULL,
	PRIMARY KEY(cpu_id),
	CONSTRAINT FK_threads FOREIGN KEY(thread_id) REFERENCES Threads(thread_id),
	CONSTRAINT FK_lithography FOREIGN KEY(lithography_id) REFERENCES Lithography(lithography_id),
	CONSTRAINT FK_launchy FOREIGN KEY(launchy_id) REFERENCES LaunchYear(launchy_id))