package consensus

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/discovery"
	"github.com/cantara/gober/sync"
	"github.com/cantara/gober/webserver"
	"github.com/cantara/gober/webserver/health"
	"github.com/gin-gonic/gin"
	"github.com/gofrs/uuid"
)

type builder struct {
	serv webserver.Server

	server
}

type ConsBuilderFunc func(topic string, consTimeout time.Duration) (cons Consensus, err error)

type ConsensusBuilder interface {
	AddTopic(topic string, consTimeout time.Duration) (cons Consensus, err error)
	Run()
}

func Init(port uint16, token string, discoverer discovery.Discoverer) (ConsensusBuilder, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	serv, err := webserver.Init(port, true)
	if err != nil {
		return nil, err
	}
	serv.API().Use(
		func(c *gin.Context) {
			auth := webserver.GetAuthHeader(c)
			if strings.TrimSpace(auth) == "" {
				c.JSON(http.StatusUnauthorized, gin.H{"status": "Unauthorized"})
				return
			}
			if auth != token {
				c.JSON(http.StatusForbidden, gin.H{"status": "FORBIDDEN"})
				return
			}
			c.Next()
		},
	)
	c := builder{
		serv: serv,
		server: server{
			port:  port,
			token: token,
			id:    id.String(),
			disc:  discoverer,
		},
	}

	return c, nil
}

func Data(c *gin.Context, code int, d any) {
	log.Debug("adding repsons data in correct format", "format", c.Accepted)
	c.JSON(code, d)
	return
}

// Solve this with a websocket instead
func (producer builder) AddTopic(t string, consTimeout time.Duration) (out Consensus, err error) {
	cons := consensus{
		server: producer.server,
		reqs:   sync.NewMap[sync.Stack[*topic]](),

		timeout: consTimeout,
		topic:   t,
	}
	producer.serv.API().GET(fmt.Sprintf("/%s/status", t), func(c *gin.Context) {
		Data(c, http.StatusOK, cons.disc.Servers())
	})
	//Get satus of a consent i know about
	producer.serv.API().GET(fmt.Sprintf("/%s/:id/consent", t), func(c *gin.Context) {
		v, ok := cons.reqs.Get(c.Param("id")[0:])
		if !ok {
			Data(c, http.StatusBadRequest, gin.H{"status": "missing consent request"})
			return
		}
		Data(c, http.StatusOK, v)
	})
	//Request consent from me
	producer.serv.API().POST(fmt.Sprintf("/%s/:id/consent", t), func(c *gin.Context) {
		id := c.Param("id")[0:]
		reqs, isNew := cons.reqs.GetOrInit(id, sync.NewStack[*topic])
		if !isNew {
			v, ok := reqs.Peek()
			if ok && time.Now().After(v.Timeout) {
				status := http.StatusConflict
				if producer.id == v.Requester {
					status = http.StatusOK
				}
				Data(c, status, consentResponse{
					Topic:     cons.topic,
					Id:        id,
					Requester: v.Requester,
					Consenter: producer.id,
				})
				return
			}
		}
		var v consentRequest
		err := c.ShouldBindJSON(&v)
		if err != nil {
			log.WithError(err).Warning("did not bind json")
			Data(c, http.StatusBadRequest, gin.H{"status": "could not unmashal data", "error": err})
			return
		}
		v.Consents = []Consent{
			{
				Id: producer.id,
				Ip: health.GetOutboundIP().String(),
			},
		}
		reqs.Push(&v.topic)
		Data(c, http.StatusCreated, consentResponse{
			Topic:     v.Topic,
			Id:        v.Id,
			Requester: v.Requester,
			Consenter: producer.id,
		})
	})
	//Has gotten updated conseed info and informs conceed winner(me)
	producer.serv.API().PATCH(fmt.Sprintf("/%s/:id/consent", t), func(c *gin.Context) {
		id := c.Param("id")[0:]
		reqs, ok := cons.reqs.Get(id)
		if !ok {
			Data(c, http.StatusNotFound, gin.H{"status": "missing consent request id"})
			return
		}
		v, ok := reqs.Peek()
		if !ok {
			Data(c, http.StatusNotFound, gin.H{"status": "missing consent request"})
			return
		}
		var data Consent
		err := c.ShouldBindJSON(&data)
		if err != nil {
			Data(c, http.StatusBadRequest, gin.H{"status": "could not unmashal data", "error": err})
			return
		}
		if containsConsent(data.Id, v.Consents) < 0 {
			v.Consents = append(v.Consents, data) //This feels wrong //Is not thread safe
		}
		Data(c, http.StatusOK, v)
		return
	})
	//Conseed to me or inform me of a conseed
	producer.serv.API().DELETE(fmt.Sprintf("/%s/:id/consent", t), func(c *gin.Context) {
		id := c.Param("id")[0:]
		reqs, ok := cons.reqs.Get(id)
		if !ok {
			Data(c, http.StatusNotFound, gin.H{"status": "missing consent request id"})
			return
		}
		v, ok := reqs.Peek()
		if !ok {
			Data(c, http.StatusNotFound, gin.H{"status": "missing consent request"})
			return
		}
		var data consentRequest
		err := c.ShouldBindJSON(&data)
		if err != nil {
			Data(c, http.StatusBadRequest, gin.H{"status": "could not unmashal data", "error": err})
			return
		}
		if producer.id == v.Requester { //Conseeding to me
			if data.Conseeding == nil {
				Data(c, http.StatusBadRequest, gin.H{"status": "not conseeding when reporting conseed"})
				return
			}
			if v.Conseeding != nil && v.Conseeding.Before(*data.Conseeding) {
				Data(c, http.StatusConflict, gin.H{"status": "conseeded too late"})
				return
			}
			v.Conseeding = nil //This seems wrong
			//Should probably remove contender

		} else if v.Requester == data.Requester { //Informing me that you have conseeded
			reqs.Pop()
			//cons.reqs.Delete(id)
		}
	})
	return &cons, nil
}

func (producer builder) Run() {
	producer.serv.Run()
}
