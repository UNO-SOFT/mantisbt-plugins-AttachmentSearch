package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/rogpeppe/retry"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/htmlindex"
)

func main() {
	if err := Main(); err != nil {
		log.Fatal(err)
	}
}

var strategy = retry.Strategy{Delay: time.Second, MaxDelay: 5 * time.Second, MaxDuration: 30 * time.Second, Factor: 1.5}

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
		"GRANT SELECT ON mantis_plugin_attachment_search_table TO mantis,public",
	} {
		if _, err := iConn.Exec(ctx, qry); err != nil {
			return fmt.Errorf("%s: %w", qry, err)
		}
	}
	const iQry = `INSERT INTO mantis_plugin_attachment_search_table (file_id, seq, meta, content, tsvec) VALUES ($1, $2, $3, $4, to_tsvector('hungarian', $4))`
	if _, err = iConn.Prepare(ctx, "insQry", iQry); err != nil {
		return fmt.Errorf("%s: %w", iQry, err)
	}

	const qry = `SELECT id, folder, diskfile, file_type FROM mantis_bug_file_table A TABLESAMPLE SYSTEM(10) 
		WHERE NOT EXISTS (SELECT 1 FROM mantis_plugin_attachment_search_table X WHERE X.file_id = A.id) AND
		      file_type NOT LIKE 'image/%' AND file_type NOT LIKE 'video/%' AND file_type NOT LIKE 'application/x-executable%' AND
		      file_type NOT IN ('application/x-java-archive')
	UNION
	SELECT id, folder, diskfile, file_type FROM mantis_bug_file_table A
		WHERE file_type NOT LIKE 'image/%' AND file_type NOT LIKE 'video/%' AND file_type NOT LIKE 'application/x-executable%' AND
		      file_type NOT IN ('application/x-java-archive') AND
		      A.id > COALESCE((SELECT MAX(file_id) FROM mantis_plugin_attachment_search_table), 0)`
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
		var folder, fn, typ string
		if err = rows.Scan(&fileID, &folder, &fn, &typ); err != nil {
			return fmt.Errorf("scan %s: %w", qry, err)
		}
		if folder != "" && strings.IndexByte(fn, '/') < 0 {
			fn = filepath.Join(folder, fn)
		}
		typ, _, _ = strings.Cut(typ, ";")
		if pre, post, found := strings.Cut(typ, "/application/"); found {
			typ = pre + "/" + post
		}
		res, err := tikaFile(ctx, *flagTika, fn, typ)
		if err != nil {
			log.Printf("tikaFile(%q, %q): %+v", fn, typ, err)
			continue
			// return fmt.Errorf("tikaFile(%q): %w", fn, err)
		}
		buf.Reset()
		for k, v := range res.Meta {
			if bytes.Contains(v, []byte("\\u0000")) {
				res.Meta[k] = bytes.ReplaceAll(v, []byte("\\u0000"), nil)
			}
		}
		if err = enc.Encode(res.Meta); err != nil {
			return err
		}
		if len(res.Content) > 20<<20 {
			res.Content = res.Content[:20<<20]
		}
		for length := 1048575; length > 16384; length /= 2 {
			content = splitAt(content[:0], res.Content, length)
			if len(content) > 100 {
				break
			}
			tx, err := iConn.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				return err
			}
			if err = func() error {
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
			tx.Rollback(ctx)
			var ec interface{ SQLState() string }
			if errors.As(err, &ec) && ec.SQLState() == "22021" {
				break
			}
			log.Printf("WARN %+v", err)
		}
	}
	err = rows.Err()
	rows.Close()
	return err
}

var errRetry = errors.New("retry")

func tikaFile(ctx context.Context, tikaURL, fn, typ string) (tikaResult, error) {
	var result tikaResult

	var resp *http.Response
	for iter := strategy.Start(); ; {
		if err := func() error {
			fh, err := os.Open(fn)
			if err != nil {
				return err
			}
			defer fh.Close()
			if typ == "" {
				var a [4096]byte
				if n, _ := fh.Read(a[:]); n != 0 {
					typ = http.DetectContentType(a[:n])
				}
				if _, err = fh.Seek(0, 0); err != nil {
					return err
				}
			}
			req, err := http.NewRequestWithContext(ctx, "PUT", tikaURL+"/tika/text", fh)
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", typ)
			req.GetBody = func() (io.ReadCloser, error) { return os.Open(fn) }
			if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode != 503 {
				return nil
			}
			log.Printf("WARN %v: %+v", req, err)
			return fmt.Errorf("%w: %w", err, errRetry)
		}(); err == nil {
			break
		} else if !errors.Is(err, errRetry) {
			return result, err
		} else if !iter.Next(ctx.Done()) {
			log.Printf("ERROR: %+v", err)
			return result, err
		}
	}
	if resp.StatusCode >= 400 {
		if resp.StatusCode == 422 {
			if result.Meta == nil {
				result.Meta = make(map[string]json.RawMessage, 1)
			}
			var err error
			result.Meta["error"], err = json.Marshal(resp.Status)
			return result, err
		}
		return result, fmt.Errorf("%s", resp.Status)
	}
	defer resp.Body.Close()
	var M map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&M); err != nil {
		return result, err
	}
	err := json.Unmarshal(M["X-TIKA:content"], &result.Content)
	delete(M, "X-TIKA:content")
	result.Meta = M
	if err != nil {
		return result, err
	}
	if b := M["X-TIKA:detectedEncoding"]; len(b) != 0 {
		var encName string
		if json.Unmarshal(b, &encName) == nil && encName != "" {
			if enc, err := htmlindex.Get(encName); err != nil {
				log.Printf("get encoding %q: %+v", encName, err)
			} else {
				if result.Content, err = enc.NewDecoder().String(result.Content); err != nil {
					log.Printf("decode from %q: %+v", encName, err)
				}
			}
		}
	}
	if !utf8.Valid([]byte(result.Content)) {
		result.Content, err = encoding.Replacement.NewEncoder().String(result.Content)
	}
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
