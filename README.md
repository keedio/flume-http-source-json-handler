# Keedio HTTP JSON handler

HTTP Source handler needed to process JSON file coming from Microsoft's SIMON desktop utility.

You can find a few JSON examples in src/test/resources/schema.

## Configuration

Add the following to your flume agent configuration:

    agent.sources = http
    
    ...
    
    agent.sources.http.type = http
    agent.sources.http.handler = com.keedio.flume.source.http.json.handler.KeedioJSONHandler
    agent.sources.http.port = <binding_port>
    agent.sources.http.enableSSL = false
    agent.sources.http.bind = <binding_host>
    
    ...
    
