package response

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

func usdResponseData(data any) (any, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err = decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var visit func(any) error
	visit = func(value any) error {
		switch values := value.(type) {
		case map[string]any:
			for key, v := range values {
				if strings.HasSuffix(key, "_usd") && v != nil {
					var amount string
					switch number := v.(type) {
					case json.Number:
						amount = number.String()
					case string:
						amount = number
					default:
						continue
					}
					rational, ok := new(big.Rat).SetString(amount)
					if !ok {
						return fmt.Errorf("invalid USD amount for %s", key)
					}
					parts := strings.SplitN(strings.ToLower(amount), "e", 2)
					exponent := 0
					if len(parts) == 2 {
						exponent, err = strconv.Atoi(parts[1])
						if err != nil {
							return err
						}
					}
					scale := 0
					if dot := strings.IndexByte(parts[0], '.'); dot >= 0 {
						scale = len(parts[0]) - dot - 1
					}
					scale -= exponent
					if scale < 0 {
						scale = 0
					}
					if scale > 1000 {
						return fmt.Errorf("USD precision out of range for %s", key)
					}
					fixed := rational.FloatString(scale)
					if strings.Contains(fixed, ".") {
						fixed = strings.TrimRight(strings.TrimRight(fixed, "0"), ".")
					}
					if fixed == "-0" {
						fixed = "0"
					}
					values[key] = fixed
				} else if err := visit(v); err != nil {
					return err
				}
			}
		case []any:
			for _, v := range values {
				if err := visit(v); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err = visit(decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

var usdDecimalInput = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)

// BindUSDJSON enforces the external decimal-string contract before Go's
// float decoder can accept exponent syntax or round excessive ledger precision.
func BindUSDJSON(c *gin.Context, target any) error {
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return err
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(raw))
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	var validate func(any) error
	validate = func(value any) error {
		switch values := value.(type) {
		case map[string]any:
			for key, v := range values {
				switch key {
				case "rate_multiplier_cny", "rate_multiplier_usd", "display_currency", "quota", "quota_used", "rate_limit_5h", "rate_limit_1d", "rate_limit_7d", "image_price_1k", "image_price_2k", "image_price_4k", "video_price_480p", "video_price_720p", "video_price_1080p", "web_search_price_per_call", "input_price", "output_price", "cache_write_price", "cache_read_price", "image_input_price", "image_output_price", "per_request_price":
					return fmt.Errorf("%s is retired; use the USD decimal-string field", key)
				}

				if strings.HasSuffix(key, "_usd") && v != nil {
					amount, ok := v.(string)
					if !ok || len(amount) > 512 || !usdDecimalInput.MatchString(amount) {
						return fmt.Errorf("%s must be a nonnegative plain USD decimal string", key)
					}
					if dot := strings.IndexByte(amount, '.'); dot >= 0 && !strings.Contains(key, "price") && len(amount)-dot-1 > 8 {
						return fmt.Errorf("%s exceeds USD amount precision", key)
					}
				} else if err := validate(v); err != nil {
					return err
				}
			}
		case []any:
			for _, v := range values {
				if err := validate(v); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := validate(value); err != nil {
		return err
	}
	return c.ShouldBindJSON(target)
}
