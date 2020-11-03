/**
 *
 */
package it.kazaam.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import it.kazaam.service.FileParserFactory;

/**
 * @author salvatore
 *
 */

@RestController
@RequestMapping("")
public class ParserController {

	@Autowired
    private FileParserFactory fileParserFactory;

    @GetMapping("/parse")
    public void parse() {
        this.fileParserFactory.parse();
    }

}
