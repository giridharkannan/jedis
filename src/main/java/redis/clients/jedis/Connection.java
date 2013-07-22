package redis.clients.jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;
import redis.clients.util.SafeEncoder;

public class Connection {
	public static Logger log = LoggerFactory.getLogger(Connection.class);
	
    private String host;
    private int port = Protocol.DEFAULT_PORT;
    private Socket socket;
    private RedisOutputStream outputStream;
    private RedisInputStream inputStream;
    private int pipelinedCommands = 0;
    private int timeout = Protocol.DEFAULT_TIMEOUT;

    public Socket getSocket() {
        return socket;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(final int timeout) {
        this.timeout = timeout;
    }

    public void setTimeoutInfinite() {
        try {
            if(!isConnected()) {
        	connect();
            }
            socket.setKeepAlive(true);
            socket.setSoTimeout(0);
        } catch (SocketException ex) {
            throw new JedisException(ex);
        }
    }

    public void rollbackTimeout() {
        try {
            socket.setSoTimeout(timeout);
            socket.setKeepAlive(false);
        } catch (SocketException ex) {
            throw new JedisException(ex);
        }
    }

    public Connection(final String host) {
        super();
        this.host = host;
    }

    protected void flush() {
    	try {
    		outputStream.flush();
    	} catch (IOException ex) {
    		close();
    		throw new JedisConnectionException(ex);
    	}
    }

    protected Connection sendCommand(final Command cmd, final String... args) {
        final byte[][] bargs = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            bargs[i] = SafeEncoder.encode(args[i]);
        }
        return sendCommand(cmd, bargs);
    }

    protected Connection sendCommand(final Command cmd, final byte[]... args) {
        connect();
        try {
	        Protocol.sendCommand(outputStream, cmd, args);
	        pipelinedCommands++;
	        return this;
        } catch(IOException e) {
        	close();
        	throw new JedisConnectionException(e);
        }
    }
    
    private void close() {
    	if(socket == null) { return; }
    	
    	try {
    		socket.close();
    		socket = null;
    	} catch(IOException e) {
    		if(log.isErrorEnabled()) {
    			log.error(String.format("error closing socket : %s" +
    					" for host : %s:%s", toString(), getHost(), getPort()), e);
    		}
    	}
    }
    
    protected Connection sendCommand(final Command cmd) {
        connect();
        try {
	        Protocol.sendCommand(outputStream, cmd, new byte[0][]);
	        pipelinedCommands++;
	        return this;
        } catch (IOException e) {
        	close();
        	throw new JedisConnectionException(e);
        }
    }

    public Connection(final String host, final int port) {
        super();
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    public Connection() {
    	
    }

    public void connect() {
        if (!isConnected()) {
            try {
                socket = new Socket();
                //->@wjw_add
                socket.setReuseAddress(true);
                socket.setKeepAlive(true);  //Will monitor the TCP connection is valid
                socket.setTcpNoDelay(true);  //Socket buffer Whetherclosed, to ensure timely delivery of data
                socket.setSoLinger(true,0);  //Control calls close () method, the underlying socket is closed immediately
                //<-@wjw_add

                socket.connect(new InetSocketAddress(host, port), timeout);
                socket.setSoTimeout(timeout);
                outputStream = new RedisOutputStream(socket.getOutputStream());
                inputStream = new RedisInputStream(socket.getInputStream());
            } catch (IOException ex) {
            	close();
                throw new JedisConnectionException(ex);
            }
        }
    }

    public void disconnect() {
        if (isConnected()) {
            try {
                inputStream.close();
                outputStream.close();
                if (!socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException ex) {
                throw new JedisConnectionException(ex);
            }
        }
    }

    public boolean isConnected() {
        return socket != null && socket.isBound() && !socket.isClosed()
                && socket.isConnected() && !socket.isInputShutdown()
                && !socket.isOutputShutdown();
    }

    protected String getStatusCodeReply() {
    	flush();
    	try {
	        pipelinedCommands--;
	        final byte[] resp = (byte[]) Protocol.read(inputStream);
	        if (null == resp) {
	            return null;
	        } else {
	            return SafeEncoder.encode(resp);
	        }
    	} catch(IOException e) {
    		close();
    		throw new JedisConnectionException(e);
    	}
    }

    public String getBulkReply() {
        final byte[] result = getBinaryBulkReply();
        if (null != result) {
            return SafeEncoder.encode(result);
        } else {
            return null;
        }
    }

    public byte[] getBinaryBulkReply() {
        flush();
    	try {
	        pipelinedCommands--;
	        return (byte[]) Protocol.read(inputStream);
    	} catch(IOException ex) {
        	close();
            throw new JedisConnectionException(ex);
    	}
    }

    public Long getIntegerReply() {
    	try {
	        flush();
	        pipelinedCommands--;
	        return (Long) Protocol.read(inputStream);
    	} catch(IOException ex) {
    		close();
    		throw new JedisConnectionException(ex);
    	}
    }

    public List<String> getMultiBulkReply() {
        return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply());
    }

    @SuppressWarnings("unchecked")
    public List<byte[]> getBinaryMultiBulkReply() {
        flush();
    	try {
	        pipelinedCommands--;
	        return (List<byte[]>) Protocol.read(inputStream);
    	} catch(IOException ex) {
    		close();
    		throw new JedisConnectionException(ex);
    	}
    }

    @SuppressWarnings("unchecked")
    public List<Object> getObjectMultiBulkReply() {
        flush();
    	try {
	        pipelinedCommands--;
	        return (List<Object>) Protocol.read(inputStream);
    	} catch(IOException ex) {
    		close();
    		throw new JedisConnectionException(ex);
    	}
    }
    
    @SuppressWarnings("unchecked")
    public List<Long> getIntegerMultiBulkReply() {
        flush();
    	try {
	        pipelinedCommands--;
	        return (List<Long>) Protocol.read(inputStream);
    	} catch(IOException ex) {
    		close();
    		throw new JedisConnectionException(ex);
    	}
    }

    public List<Object> getAll() {
        return getAll(0);
    }

    public List<Object> getAll(int except) {
        List<Object> all = new ArrayList<Object>();
        flush();
        try {
	        while (pipelinedCommands > except) {
	        	try{
	                all.add(Protocol.read(inputStream));
	        	}catch(JedisDataException e){
	        		all.add(e);
	        	}
	            pipelinedCommands--;
	        }
	        return all;
        } catch(IOException ex) {
    		close();
    		throw new JedisConnectionException(ex);
    	}
    }

    public Object getOne() {
        flush();
    	try {
	        pipelinedCommands--;
	        return Protocol.read(inputStream);
    	} catch(IOException ex) {
    		close();
    		throw new JedisConnectionException(ex);
    	}
    }
}
