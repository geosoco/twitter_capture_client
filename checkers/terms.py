#!/usr/bin/env python
"""Term checker helper."""

import logging


class TermChecker(object):

    """Term Checker"""

    def __init__(self, server_messenger):
        """Initialize the termchecker."""
        self.server_messenger = server_messenger

        self.current_terms = []
        self.current_terms_set = set()

        self.terms_changed = False

        self.log = logging.getLogger("TermChecker")

    def configure(self, config):
        """configure the term checker."""
        pass

    def requestTerms(self):
        """request terms from server. used internally."""

        status_msg = self.server_messenger.getStatus()
        keywords = status_msg.get('twitter_keywords', None)
        return (
            [kw.strip() for kw in
                keywords.split(",")]
            if keywords else None)

    def checkTerms(self):
        """check and update flags based on terms."""

        # request new terms from server
        new_terms = self.requestTerms()
        new_terms_set = set(new_terms)

        # detail what type of differences
        if new_terms_set != self.current_terms_set:
                self.log.info("twitter filter words changed: ")
                subtractions = self.current_terms_set - new_terms_set
                additions = new_terms_set - self.current_terms_set
                self.log.info("    + : %s", repr(additions))
                self.log.info("    - : %s", repr(subtractions))

                self.terms_changed = True
                self.current_terms = new_terms
                self.current_terms_set = set(self.current_terms)




    def haveTermsChanged(self):
        """return true if terms have changed"""
        return self.terms_changed

    def resetTermsChanged(self):
        """return true if terms have changed"""
        self.terms_changed = False

    @property
    def terms(self):
        return self.current_terms
