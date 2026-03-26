package io.rcardin.spring.kafka.stream;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public MessageConverter jsonMessageConverter() {
		return new Jackson2JsonMessageConverter();
	}

	@Bean
	public Queue wordsQueue() {
		return new Queue("words", false);
	}

	@Bean
	public Queue wordCountersQueue() {
		return new Queue("word-counters", false);
	}

	@Bean
	public TopicExchange exchange() {
		return new TopicExchange("app.topic");
	}

	@Bean
	public Binding binding(Queue wordsQueue, TopicExchange exchange) {
		return BindingBuilder.bind(wordsQueue).to(exchange).with("words");
	}

	@Bean
	public Binding binding2(Queue wordCountersQueue, TopicExchange exchange) {
		return BindingBuilder.bind(wordCountersQueue).to(exchange).with("word-counters");
	}
}

@Service
@Transactional
class WordCountService {

	private final RabbitTemplate rabbitTemplate;
	private final WordCountRepository wordCountRepository;

	public WordCountService(RabbitTemplate rabbitTemplate, WordCountRepository wordCountRepository) {
		this.rabbitTemplate = rabbitTemplate;
		this.wordCountRepository = wordCountRepository;
	}

	public void processWords(String message) {
		String[] words = message.split(" ");
		for (String word : words) {
			WordCount wordCount = wordCountRepository.findById(word).orElse(null);
			if (wordCount == null) {
				wordCount = new WordCount(word, 0L);
				wordCountRepository.save(wordCount);
			}
			wordCount.setCount(wordCount.getCount() + 1);
			wordCountRepository.save(wordCount);
			rabbitTemplate.convertAndSend("word-counters", new WordCount(word, wordCount.getCount()));
		}
	}
}

@Entity
class WordCount {
	@Id
	private String word;
	private Long count;

	public WordCount() {}

	public WordCount(String word, Long count) {
		this.word = word;
		this.count = count;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}
}

interface WordCountRepository extends JpaRepository<WordCount, String> {
}