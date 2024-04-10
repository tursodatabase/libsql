CREATE TABLE Item(
a integer PRIMARY KEY NOT NULL ,
b double NULL ,
c int NOT NULL DEFAULT 0
);
CREATE TABLE Undo(UndoAction TEXT);
INSERT INTO Item VALUES (1,38205.60865,340);
CREATE TRIGGER trigItem_UNDO_AD AFTER DELETE ON Item FOR EACH ROW
BEGIN
INSERT INTO Undo SELECT 'INSERT INTO Item (a,b,c) VALUES ('
|| coalesce(old.a,'NULL') || ',' || quote(old.b) || ',' || old.c || ');';
END;
DELETE FROM Item WHERE a = 1;
SELECT * FROM Undo;