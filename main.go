package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/jackc/pgx/v5"
)

func main() {
	if err := Main(); err != nil {
		log.Fatal(err)
	}
}

func Main() error {
	flagTika := flag.String("tika", "http://localhost:9998", "Tika URL")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	connString := flag.Arg(0)
	if !strings.Contains(connString, "database=") && strings.IndexByte(connString, ' ') < 0 {
		connString = "database=" + connString
	}
	qConn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("connect to %q: %w", connString, err)
	}
	defer qConn.Close(context.Background())

	iConn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer iConn.Close(context.Background())
	for _, qry := range []string{
		"CREATE TABLE IF NOT EXISTS mantis_plugin_attachment_search_table (file_id INTEGER, seq SMALLINT, meta JSONB, content TEXT, tsvec TSVECTOR)",
		"CREATE INDEX IF NOT EXISTS mantis_plugin_attachment_search_table_idx_file_id ON mantis_plugin_attachment_search_table(file_id)",
		"CREATE INDEX IF NOT EXISTS mantis_plugin_attachment_search_table_idx_tsvec ON mantis_plugin_attachment_search_table USING GIN (tsvec)",
	} {
		if _, err := iConn.Exec(ctx, qry); err != nil {
			return fmt.Errorf("%s: %w", qry, err)
		}
	}
	const iQry = `INSERT INTO mantis_plugin_attachment_search_table (file_id, seq, meta, content, tsvec) VALUES ($1, $2, $3, $4, to_tsvector('hungarian', $4))`
	if _, err = iConn.Prepare(ctx, "insQry", iQry); err != nil {
		return fmt.Errorf("%s: %w", iQry, err)
	}

	const qry = `SELECT id, folder||diskfile, file_type FROM mantis_bug_file_table A
		WHERE NOT EXISTS (SELECT 1 FROM mantis_plugin_attachment_search_table X WHERE X.file_id = A.id) AND
		      file_type NOT LIKE 'image/%' AND file_type NOT LIKE 'video/%' AND file_type NOT LIKE 'application/x-executable%' AND
		      file_type NOT IN ('application/x-java-archive')`
	rows, err := qConn.Query(ctx, qry)
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	defer rows.Close()
	var buf bytes.Buffer
	content := make([]string, 0, 10)
	enc := json.NewEncoder(&buf)
	for rows.Next() {
		var fileID int
		var fn, typ string
		if err = rows.Scan(&fileID, &fn, &typ); err != nil {
			return fmt.Errorf("scan %s: %w", qry, err)
		}
		typ, _, _ = strings.Cut(typ, ";")
		res, err := tikaFile(ctx, *flagTika, fn, typ)
		if err != nil {
			log.Printf("tikaFile(%q, %q): %+v", fn, typ, err)
			continue
			// return fmt.Errorf("tikaFile(%q): %w", fn, err)
		}
		buf.Reset()
		if err = enc.Encode(res.Meta); err != nil {
			return err
		}
		if len(res.Content) > 20<<20 {
			res.Content = res.Content[:20<<20]
		}
		for length := 1048575; length > 1024; length /= 2 {
			tx, err := iConn.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)
			if err := func() error {
				content = splitAt(content[:0], res.Content, length)
				for i, c := range content {
					log.Println(fileID, i, len(c))
					if _, err := iConn.Exec(ctx, "insQry", fileID, i, buf.Bytes(), c); err != nil {
						return fmt.Errorf("%s [%d, %d, %q, %d]: %w", iQry, fileID, i, buf.String(), len(c), err)
					}
				}
				return nil
			}(); err == nil {
				if err := tx.Commit(ctx); err != nil {
					return err
				}
				break
			}
		}
	}
	err = rows.Err()
	rows.Close()
	return err
}

func tikaFile(ctx context.Context, tikaURL, fn, typ string) (tikaResult, error) {
	var result tikaResult
	fh, err := os.Open(fn)
	if err != nil {
		return result, err
	}
	defer fh.Close()
	req, err := http.NewRequestWithContext(ctx, "PUT", tikaURL+"/tika/text", fh)
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", typ)
	resp, err := http.DefaultClient.Do(req)
	fh.Close()
	if err != nil {
		return result, err
	}
	if resp.StatusCode >= 400 {
		if resp.StatusCode == 422 {
			if result.Meta == nil {
				result.Meta = make(map[string]json.RawMessage, 1)
			}
			result.Meta["error"], err = json.Marshal(resp.Status)
			return result, err
		}
		return result, fmt.Errorf("%s", resp.Status)
	}
	defer resp.Body.Close()
	var M map[string]json.RawMessage
	if err = json.NewDecoder(resp.Body).Decode(&M); err != nil {
		return result, err
	}
	err = json.Unmarshal(M["X-TIKA:content"], &result.Content)
	delete(M, "X-TIKA:content")
	result.Meta = M
	return result, err
}

type tikaResult struct {
	Content string `json:"X-TIKA:content"`
	Meta    map[string]json.RawMessage
}

func splitAt(dest []string, text string, length int) []string {
	s := text
	for len(s) > length {
		if i := strings.LastIndexAny(s[:length], "\n\t\r "); i > 0 {
			dest = append(dest, s[:i+1])
			s = s[i+1:]
		} else {
			dest = append(dest, s[:length])
			s = s[length:]
		}
	}
	return append(dest, s)
}
