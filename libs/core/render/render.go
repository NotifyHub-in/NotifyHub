package render

import (
	"fmt"
	"sort"
	"strings"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
)

func Subject(record notification.NotificationRecord) string {
	return fmt.Sprintf("%s:%s", record.EventName, record.TemplateKey)
}

func Body(record notification.NotificationRecord, channel notification.Channel) string {
	keys := make([]string, 0, len(record.Variables))
	for key := range record.Variables {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("event=%s\n", record.EventName))
	builder.WriteString(fmt.Sprintf("template=%s\n", record.TemplateKey))
	builder.WriteString(fmt.Sprintf("channel=%s\n", channel))
	for _, key := range keys {
		builder.WriteString(fmt.Sprintf("%s=%s\n", key, record.Variables[key]))
	}
	return builder.String()
}
